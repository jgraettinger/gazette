package protocol

import (
	epb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

func (m Header) Validate() error {
	if err := m.BrokerId.Validate(); err != nil {
		return ExtendContext(err, "Id")
	} else if err := m.Route.Validate(); err != nil {
		return ExtendContext(err, "Route")
	}
	if m.ProxyId != nil {
		if err := m.ProxyId.Validate(); err != nil {
			return ExtendContext(err, "ProxyId")
		}
	}
	return nil
}

func FromEtcdResponseHeader(h epb.ResponseHeader) Header_Etcd {
	return Header_Etcd{
		ClusterId: h.ClusterId,
		MemberId:  h.MemberId,
		Revision:  h.Revision,
		RaftTerm:  h.RaftTerm,
	}
}
