package remote

import (
	"context"
	"errors"
	"io"

	"github.com/avivklas/plexus-flow/pkg/remote/workerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements workerpb.WorkerServiceServer on top of a Broker.
type Server struct {
	workerpb.UnimplementedWorkerServiceServer
	broker *Broker
}

// NewServer wraps a broker in the gRPC service.
func NewServer(b *Broker) *Server { return &Server{broker: b} }

// Register adds the service to a gRPC server.
func (s *Server) Register(g grpc.ServiceRegistrar) { workerpb.RegisterWorkerServiceServer(g, s) }

// Connect runs one worker session.
func (s *Server) Connect(stream grpc.BidiStreamingServer[workerpb.WorkerMessage, workerpb.ServerMessage]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	reg := first.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "the first message must be Register")
	}
	sess, err := s.broker.Attach(reg.GetWorkerId(), reg.GetActivities(), int(reg.GetMaxConcurrency()))
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	defer sess.Close()

	if err := stream.Send(&workerpb.ServerMessage{Message: &workerpb.ServerMessage_Registered{
		Registered: &workerpb.Registered{HeartbeatIntervalMs: sess.HeartbeatInterval().Milliseconds()}}}); err != nil {
		return err
	}

	sendDone := make(chan error, 1)
	go func() {
		for m := range sess.Out() {
			if err := stream.Send(m); err != nil {
				sendDone <- err
				return
			}
		}
		sendDone <- nil // the broker closed the session (lost worker, shutdown)
	}()

	recvErr := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			switch x := m.Message.(type) {
			case *workerpb.WorkerMessage_Heartbeat:
				sess.Heartbeat()
			case *workerpb.WorkerMessage_Result:
				if err := sess.Result(x.Result); err != nil {
					recvErr <- status.Error(codes.InvalidArgument, err.Error())
					return
				}
			case *workerpb.WorkerMessage_Register:
				recvErr <- status.Error(codes.InvalidArgument, "already registered")
				return
			}
		}
	}()

	select {
	case err := <-recvErr:
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case err := <-sendDone:
		if err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "session closed by the server (heartbeat missed or shutdown)")
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}
