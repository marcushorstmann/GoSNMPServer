package GoSNMPServer

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

type ISnmpServerListener interface {
	SetupLogger(ILogger)
	Address() net.Addr
	NextSnmp() (snmpbytes []byte, replyer IReplyer, err error)
	Shutdown()
	SetReadDeadline(t time.Time) error
}

type IReplyer interface {
	ReplyPDU([]byte) error
	Shutdown()
}

type UDPListener struct {
	conn   *net.UDPConn
	logger ILogger
}

func NewUDPListener(address string, opts *UDPOptions) (ISnmpServerListener, error) {
	ret := new(UDPListener)
	ret.logger = NewDiscardLogger()
	conn, err := listenUDPInternal(context.Background(), address, opts)
	if err != nil {
		return nil, errors.Wrap(err, "UDP Listen Error")
	}
	ret.conn = conn
	return ret, nil
}

func listenUDPInternal(ctx context.Context, address string, opts *UDPOptions) (*net.UDPConn, error) {
	if opts == nil {
		opts = &UDPOptions{
			L3Proto: "udp",
			ToS:     0,
		}
	}
	var listenConfig net.ListenConfig
	if opts.ToS != 0 {
		listenConfig = net.ListenConfig{
			Control: func(_, _ string, c syscall.RawConn) error {
				var err error
				controlErr := c.Control(func(fd uintptr) {
					err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, opts.ToS)
				})
				if err != nil {
					return fmt.Errorf("error while setting sockopt for ToS: %w", err)
				}
				if controlErr != nil {
					return fmt.Errorf("error while ctrl func for setting sockopt for ToS: %w", controlErr)
				}
				return nil
			},
		}
	} else {
		listenConfig = net.ListenConfig{}
	}
	conn, err := listenConfig.ListenPacket(ctx, opts.L3Proto, address)
	if err != nil {
		return nil, fmt.Errorf("error while listening for packet: %w", err)
	}
	udpconn, isone := conn.(*net.UDPConn)
	if !isone {
		return nil, errors.New("is not an udp connection type")
	}
	return udpconn, nil
}

func (udp *UDPListener) SetupLogger(i ILogger) {
	udp.logger = i
}
func (udp *UDPListener) Address() net.Addr {
	return udp.conn.LocalAddr()
}

func (udp *UDPListener) NextSnmp() ([]byte, IReplyer, error) {
	var msg [4096]byte
	if udp.conn == nil {
		return nil, nil, errors.New("Connection Not Listen")
	}
	counts, udpAddr, err := udp.conn.ReadFromUDP(msg[:])
	if err != nil {
		return nil, nil, errors.Wrap(err, "UDP Read Error")
	}
	udp.logger.Debugf("udp request from %v. size=%v", udpAddr, counts)
	return msg[:counts], &UDPReplyer{udpAddr, udp.conn}, nil
}

func (udp *UDPListener) Shutdown() {
	if udp.conn != nil {
		udp.conn.Close()
		udp.conn = nil
	}
}

func (udp *UDPListener) SetReadDeadline(t time.Time) error {
	return udp.conn.SetReadDeadline(t)
}

type UDPReplyer struct {
	target *net.UDPAddr
	conn   *net.UDPConn
}

func (r *UDPReplyer) ReplyPDU(i []byte) error {
	conn := r.conn
	_, err := conn.WriteToUDP(i, r.target)
	if err != nil {
		return errors.Wrap(err, "WriteToUDP")
	}
	return nil
}

func (r *UDPReplyer) Shutdown() {}
