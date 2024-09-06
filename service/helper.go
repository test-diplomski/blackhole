package service

import (
	"errors"
	"fmt"
	"github.com/c12s/blackhole/helper"
	aPb "github.com/c12s/scheme/apollo"
	bPb "github.com/c12s/scheme/blackhole"
	mPb "github.com/c12s/scheme/meridian"
	sg "github.com/c12s/stellar-go"
	"golang.org/x/net/context"
	"strings"
)

func (s *Server) auth(ctx context.Context, opt *aPb.AuthOpt) error {
	span, _ := sg.FromGRPCContext(ctx, "auth")
	defer span.Finish()
	fmt.Println(span)

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return err
	}

	client := NewApolloClient(s.Apollo)
	resp, err := client.Auth(
		helper.AppendToken(
			sg.NewTracedGRPCContext(ctx, span),
			token,
		),
		opt,
	)
	if err != nil {
		span.AddLog(&sg.KV{"apollo resp error", err.Error()})
		return err
	}

	if !resp.Value {
		span.AddLog(&sg.KV{"apollo.auth value", resp.Data["message"]})
		return errors.New(resp.Data["message"])
	}
	return nil
}

func (s *Server) checkNS(ctx context.Context, userid, namespace string) (string, error) {
	span, _ := sg.FromGRPCContext(ctx, "ns check")
	defer span.Finish()
	fmt.Println(span)

	client := NewMeridianClient(s.Meridian)
	mrsp, err := client.Exists(sg.NewTracedGRPCContext(ctx, span),
		&mPb.NSReq{
			Name:   namespace,
			Extras: map[string]string{"userid": userid},
		},
	)
	if err != nil {
		span.AddLog(&sg.KV{"meridian exists error", err.Error()})
		return "", err
	}

	if mrsp.Extras["exists"] == "" {
		fmt.Println("namespace do not exists")
		return "", errors.New(fmt.Sprintf("%s do not exists", namespace))
	}
	fmt.Println("namespace exists")
	return mrsp.Extras["exists"], nil
}

func mutateKind(kind bPb.TaskKind) string {
	switch kind {
	case bPb.TaskKind_SECRETS:
		return "secrets"
	case bPb.TaskKind_ACTIONS:
		return "actions"
	case bPb.TaskKind_CONFIGS:
		return "configs"
	case bPb.TaskKind_NAMESPACES:
		return "namespaces"
	case bPb.TaskKind_ROLES:
		return "roles"
	}
	return ""
}

func compareKind(kind bPb.CompareKind) string {
	switch kind {
	case bPb.CompareKind_ALL:
		return "kind:all"
	case bPb.CompareKind_ANY:
		return "kind:any"
	}
	return ""
}

func join(data map[string]string) string {
	temp := []string{}
	for lk, lv := range data {
		temp = append(temp, strings.Join([]string{lk, lv}, ":"))
	}
	return strings.Join(temp, ",")
}

func mutateOpt(req *bPb.PutReq, token string) *aPb.AuthOpt {
	retval := &aPb.AuthOpt{
		Data: map[string]string{
			"intent":    "auth",
			"action":    "mutate",
			"kind":      mutateKind(req.Kind),
			"user":      req.UserId,
			"token":     token,
			"namespace": req.Mtdata.Namespace,
		},
	}

	switch req.Kind {
	case bPb.TaskKind_SECRETS, bPb.TaskKind_CONFIGS, bPb.TaskKind_ACTIONS:
		retval.Extras = map[string]*aPb.OptExtras{}
		for _, t := range req.Tasks {
			retval.Extras[t.RegionId] = &aPb.OptExtras{
				Data: []string{t.ClusterId,
					join(t.Selector.Labels),
					compareKind(t.Selector.Kind),
				},
			}
		}
	case bPb.TaskKind_NAMESPACES:
		retval.Extras = map[string]*aPb.OptExtras{
			"labels":    &aPb.OptExtras{Data: []string{req.Extras["labels"]}},
			"namespace": &aPb.OptExtras{Data: []string{req.Extras["namespace"]}},
		}
	case bPb.TaskKind_ROLES:
		retval.Extras = map[string]*aPb.OptExtras{
			"user":     &aPb.OptExtras{Data: []string{req.Extras["user"]}},
			"resource": &aPb.OptExtras{Data: []string{req.Extras["resources"]}},
			"verbs":    &aPb.OptExtras{Data: []string{req.Extras["verbs"]}},
		}
	}

	return retval
}
