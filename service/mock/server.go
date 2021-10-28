// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/ONSdigital/dp-search-data-importer/service"
	"sync"
)

var (
	lockHTTPServerMockListenAndServe sync.RWMutex
	lockHTTPServerMockShutdown       sync.RWMutex
)

// Ensure, that HTTPServerMock does implement HTTPServer.
// If this is not the case, regenerate this file with moq.
var _ service.HTTPServer = &HTTPServerMock{}

// HTTPServerMock is a mock implementation of service.HTTPServer.
//
//     func TestSomethingThatUsesHTTPServer(t *testing.T) {
//
//         // make and configure a mocked service.HTTPServer
//         mockedHTTPServer := &HTTPServerMock{
//             ListenAndServeFunc: func() error {
// 	               panic("mock out the ListenAndServe method")
//             },
//             ShutdownFunc: func(ctx context.Context) error {
// 	               panic("mock out the Shutdown method")
//             },
//         }
//
//         // use mockedHTTPServer in code that requires service.HTTPServer
//         // and then make assertions.
//
//     }
type HTTPServerMock struct {
	// ListenAndServeFunc mocks the ListenAndServe method.
	ListenAndServeFunc func() error

	// ShutdownFunc mocks the Shutdown method.
	ShutdownFunc func(ctx context.Context) error

	// calls tracks calls to the methods.
	calls struct {
		// ListenAndServe holds details about calls to the ListenAndServe method.
		ListenAndServe []struct {
		}
		// Shutdown holds details about calls to the Shutdown method.
		Shutdown []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
	}
}

// ListenAndServe calls ListenAndServeFunc.
func (mock *HTTPServerMock) ListenAndServe() error {
	if mock.ListenAndServeFunc == nil {
		panic("HTTPServerMock.ListenAndServeFunc: method is nil but HTTPServer.ListenAndServe was just called")
	}
	callInfo := struct {
	}{}
	lockHTTPServerMockListenAndServe.Lock()
	mock.calls.ListenAndServe = append(mock.calls.ListenAndServe, callInfo)
	lockHTTPServerMockListenAndServe.Unlock()
	return mock.ListenAndServeFunc()
}

// ListenAndServeCalls gets all the calls that were made to ListenAndServe.
// Check the length with:
//     len(mockedHTTPServer.ListenAndServeCalls())
func (mock *HTTPServerMock) ListenAndServeCalls() []struct {
} {
	var calls []struct {
	}
	lockHTTPServerMockListenAndServe.RLock()
	calls = mock.calls.ListenAndServe
	lockHTTPServerMockListenAndServe.RUnlock()
	return calls
}

// Shutdown calls ShutdownFunc.
func (mock *HTTPServerMock) Shutdown(ctx context.Context) error {
	if mock.ShutdownFunc == nil {
		panic("HTTPServerMock.ShutdownFunc: method is nil but HTTPServer.Shutdown was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	lockHTTPServerMockShutdown.Lock()
	mock.calls.Shutdown = append(mock.calls.Shutdown, callInfo)
	lockHTTPServerMockShutdown.Unlock()
	return mock.ShutdownFunc(ctx)
}

// ShutdownCalls gets all the calls that were made to Shutdown.
// Check the length with:
//     len(mockedHTTPServer.ShutdownCalls())
func (mock *HTTPServerMock) ShutdownCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	lockHTTPServerMockShutdown.RLock()
	calls = mock.calls.Shutdown
	lockHTTPServerMockShutdown.RUnlock()
	return calls
}
