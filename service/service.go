package service

import (
	"go/ast"
	"log"
	"reflect"
)

// methodType stores metadata about RPC method
type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
}

// newArgv creates a new argument value for the method
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		// Allocate underlying element for pointer type
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// Create direct value for non-pointer type
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// newReplyv creates a new reply value for the method
func (m *methodType) newReplyv() reflect.Value {
	// ReplyType is expected to be a pointer type
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

// service represents RPC service
type service struct {
	name      string
	typ       reflect.Type
	rev       reflect.Value
	methodMap map[string]*methodType
}

func newService(rev interface{}) *service {
	s := &service{
		typ: reflect.TypeOf(rev),
		rev: reflect.ValueOf(rev),
	}
	s.name = reflect.Indirect(s.rev).Type().Name()
	if !ast.IsExported(s.name) {
		log.Fatalf("%s is not exported", s.name)
	}
	s.registerMethods()
	return s
}

// registerMethods scans and registers RPC methods
// func (t *T) MethodName(arg T1, reply *T2) error
func (s *service) registerMethods() {
	s.methodMap = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.methodMap[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
	}
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// call invokes the RPC method dynamically using reflection
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	returnValue := m.method.Func.Call([]reflect.Value{s.rev, argv, replyv})
	if err := returnValue[0].Interface(); err != nil {
		return err.(error)
	}
	return nil
}
