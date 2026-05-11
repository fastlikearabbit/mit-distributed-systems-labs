package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"
	ErrTxn     = "ErrTxn"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

type TxnCompareTarget string

const (
	TxnCompareVersion TxnCompareTarget = "version"
	TxnCompareValue   TxnCompareTarget = "value"
)

type TxnCompareResult string

const (
	TxnCompareEqual    TxnCompareResult = "="
	TxnCompareNotEqual TxnCompareResult = "!="
	TxnCompareGreater  TxnCompareResult = ">"
	TxnCompareLess     TxnCompareResult = "<"
)

type TxnCompare struct {
	Key     string
	Target  TxnCompareTarget
	Result  TxnCompareResult
	Value   string
	Version Tversion
}

func CmpVersion(key string, result TxnCompareResult, version Tversion) TxnCompare {
	return TxnCompare{Key: key, Target: TxnCompareVersion, Result: result, Version: version}
}

func CmpValue(key string, result TxnCompareResult, value string) TxnCompare {
	return TxnCompare{Key: key, Target: TxnCompareValue, Result: result, Value: value}
}

type TxnOpKind string

const (
	TxnOpGet TxnOpKind = "get"
	TxnOpPut TxnOpKind = "put"
)

type TxnOp struct {
	Kind  TxnOpKind
	Key   string
	Value string
}

func OpGet(key string) TxnOp {
	return TxnOp{Kind: TxnOpGet, Key: key}
}

func OpPut(key, value string) TxnOp {
	return TxnOp{Kind: TxnOpPut, Key: key, Value: value}
}

type TxnArgs struct {
	Compare []TxnCompare
	Then    []TxnOp
	Else    []TxnOp
}

type TxnOpResponse struct {
	Kind    TxnOpKind
	Value   string
	Version Tversion
	Err     Err
}

type TxnReply struct {
	Succeeded bool
	Responses []TxnOpResponse
	Err       Err
}
