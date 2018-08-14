package connectors

import (
	"github.com/shopspring/decimal"
	"github.com/lightningnetwork/lnd/lnrpc"
)

type LightningInfo struct {
	Host      string
	Port      string
	MinAmount string
	MaxAmount string
	*lnrpc.GetInfoResponse
}

// GeneratedTransaction...
type GeneratedTransaction interface {
	// ID...
	ID() string

	// Bytes...
	Bytes() []byte
}

type NetworkType string

var (
	Blockchain NetworkType = "blockchain"
	Lightning  NetworkType = "lightning"
)

// Payment is the structure which describe the action of funds
// movement from one User to another.
type Payment struct {
	// ID is an number which identifies the transaction inside the payment
	// system.
	ID string

	// Amount is an number of money which is translated from one User to
	// another in this transaction.
	Amount decimal.Decimal

	// Account is the receiver account.
	Account string

	// Address is an address of receiver.
	Address string

	// Type is a type of network which is used to deliver the payment.
	Type NetworkType
}

// BlockchainPendingPayment is the transaction with confirmations number lower
// than required by the payment system to treated them as confirmed.
type BlockchainPendingPayment struct {
	Payment

	// Confirmations is the number of confirmations.
	Confirmations int64

	// ConfirmationsLeft is the number of confirmations left in order to
	// interpret the transaction as confirmed.
	ConfirmationsLeft int64
}

// BlockchainConnector is an interface which describes the blockchain service
// which is able to connect to blockchain daemon of particular currency and
// operate with transactions, addresses, and also  able to notify other
// subsystems when transaction passes required number of confirmations.
type BlockchainConnector interface {
	// CreateAddress is used to create deposit address.
	CreateAddress(account string) (string, error)

	// AccountAddress return the deposit address of account.
	AccountAddress(account string) (string, error)

	// ConfirmedBalance return the amount of confirmed funds available for account.
	ConfirmedBalance(account string) (decimal.Decimal, error)

	// PendingBalance return the amount of funds waiting to be confirmed.
	PendingBalance(account string) (decimal.Decimal, error)

	// PendingTransactions return the transactions which has confirmation
	// number lower the required by payment system.
	PendingTransactions(account string) ([]*BlockchainPendingPayment, error)

	// GenerateTransaction generates raw blockchain transaction.
	GenerateTransaction(address string, amount string) (GeneratedTransaction, error)

	// SendTransaction sends the given transaction to the blockchain network.
	SendTransaction(rawTx []byte) error

	// ReceivedPayments returns channel with transactions which are passed
	// the minimum threshold required by the client to treat the
	// transactions as confirmed.
	ReceivedPayments() <-chan []*Payment

	// ValidateAddress takes the blockchain address and ensure its valid.
	ValidateAddress(address string) error
}

// LightningConnector is an interface which describes the service
// which is able to connect lightning network daemon of particular currency and
// operate with transactions, addresses, and also  able to notify other
// subsystems when invoice is settled.
type LightningConnector interface {
	// Info returns the information about our lnd node.
	Info() (*LightningInfo, error)

	// CreateInvoice is used to create lightning network invoice.
	CreateInvoice(account string, amount string) (string, error)

	// SendTo is used to send specific amount of money to address within this
	// payment system.
	SendTo(invoice string) error

	// ConfirmedBalance return the amount of confirmed funds available for account.
	// TODO(andrew.shvv) Implement lightning wallet balance
	ConfirmedBalance(account string) (decimal.Decimal, error)

	// PendingBalance return the amount of funds waiting to be confirmed.
	// TODO(andrew.shvv) Implement lightning wallet balance
	PendingBalance(account string) (decimal.Decimal, error)

	// ReceivedPayments returns channel with transactions which are passed
	// the minimum threshold required by the client to treat as confirmed.
	ReceivedPayments() <-chan *Payment

	// QueryRoutes returns list of routes from to the given lnd node,
	// and insures the the capacity of the channels is sufficient.
	QueryRoutes(pubKey, amount string, limit int32) ([]*lnrpc.Route, error)

	// ValidateInvoice takes the encoded lightning network invoice and ensure
	// its valid.
	ValidateInvoice(invoice, amount string) error
}
