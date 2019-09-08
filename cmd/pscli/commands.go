package main

import (
	"fmt"
	"github.com/bitlum/connector/crpc"
	"github.com/go-errors/errors"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
	"strings"
)

func printRespJSON(resp proto.Message) {
	jsonMarshaler := &jsonpb.Marshaler{
		EmitDefaults: true,
		Indent:       "    ",
	}

	jsonStr, err := jsonMarshaler.MarshalToString(resp)
	if err != nil {
		fmt.Println("unable to decode response: ", err)
		return
	}

	fmt.Println(jsonStr)
}

var createReceiptCommand = cli.Command{
	Name:     "createreceipt",
	Category: "Receipt",
	Usage:    "Generates new receipt.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "asset",
			Usage: "Asset is an acronym of the crypto currency",
		},
		cli.StringFlag{
			Name: "media",
			Usage: "Media is a type of technology which is used to transport" +
				" value of underlying asset",
		},
		cli.StringFlag{
			Name: "amount",
			Usage: "(optional) Amount is the amount which should be received on this " +
				"receipt.",
		},
		cli.StringFlag{
			Name: "description",
			Usage: "(optional) Description works only for lightning invoices." +
				" This description will be placed in the invoice itself, " +
				"which would allow user to see what he paid for later in" +
				" the wallet.",
		},
	},
	Action: createReceipt,
}

func createReceipt(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var (
		media       crpc.Media
		asset       crpc.Asset
		amount      string
		description string
	)

	switch {
	case ctx.IsSet("media"):
		stringMedia := ctx.String("media")
		switch stringMedia {
		case "blockchain":
			media = crpc.Media_BLOCKCHAIN
		case "lightning":
			media = crpc.Media_LIGHTNING
		default:
			return errors.Errorf("invalid media type %v, support media type "+
				"are: 'blockchain' and 'lightning'", stringMedia)
		}
	default:
		return errors.New("media argument missing")
	}

	switch {
	case ctx.IsSet("asset"):
		stringAsset := strings.ToLower(ctx.String("asset"))
		switch stringAsset {
		case "btc", "bitcoin":
			asset = crpc.Asset_BTC
		case "bch", "bitcoincash":
			asset = crpc.Asset_BCH
		case "ltc", "litecoin":
			asset = crpc.Asset_LTC
		case "eth", "ethereum":
			asset = crpc.Asset_ETH
		case "dash":
			asset = crpc.Asset_DASH
		default:
			return errors.Errorf("invalid asset %v, supported assets"+
				"are: 'btc', 'bch', 'dash', 'eth', 'ltc'", stringAsset)
		}
	default:
		return errors.Errorf("asset argument missing")
	}

	if ctx.IsSet("amount") {
		amount = ctx.String("amount")
	}

	if ctx.IsSet("description") {
		description = ctx.String("description")
	}

	ctxb := context.Background()
	resp, err := client.CreateReceipt(ctxb, &crpc.CreateReceiptRequest{
		Asset:       asset,
		Media:       media,
		Amount:      amount,
		Description: description,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}

var validateReceiptCommand = cli.Command{
	Name:     "validatereceipt",
	Category: "Receipt",
	Usage:    "Validates given receipt.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "receipt",
			Usage: "Receipt is either blockchain address or lightning network.",
		},
		cli.StringFlag{
			Name:  "asset",
			Usage: "Asset is an acronym of the crypto currency",
		},
		cli.StringFlag{
			Name: "media",
			Usage: "Media is a type of technology which is used to transport" +
				" value of underlying asset",
		},
		cli.StringFlag{
			Name: "amount",
			Usage: "(optional) Amount is the amount which should be received on this " +
				"receipt.",
		},
	},
	Action: validateReceipt,
}

func validateReceipt(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var (
		media   crpc.Media
		asset   crpc.Asset
		amount  string
		receipt string
	)

	switch {
	case ctx.IsSet("media"):
		stringMedia := ctx.String("media")
		switch stringMedia {
		case "blockchain":
			media = crpc.Media_BLOCKCHAIN
		case "lightning":
			media = crpc.Media_LIGHTNING
		default:
			return errors.Errorf("invalid media type %v, support media type "+
				"are: 'blockchain' and 'lightning'", stringMedia)
		}
	default:
		return errors.New("media argument missing")
	}

	switch {
	case ctx.IsSet("asset"):
		stringAsset := strings.ToLower(ctx.String("asset"))
		switch stringAsset {
		case "btc", "bitcoin":
			asset = crpc.Asset_BTC
		case "bch", "bitcoincash":
			asset = crpc.Asset_BCH
		case "ltc", "litecoin":
			asset = crpc.Asset_LTC
		case "eth", "ethereum":
			asset = crpc.Asset_ETH
		case "dash":
			asset = crpc.Asset_DASH
		default:
			return errors.Errorf("invalid asset %v, supported assets"+
				"are: 'btc', 'bch', 'dash', 'eth', 'ltc'", stringAsset)
		}
	default:
		return errors.Errorf("asset argument missing")
	}

	if ctx.IsSet("amount") {
		amount = ctx.String("amount")
	}

	if ctx.IsSet("receipt") {
		receipt = ctx.String("receipt")
	} else {
		return errors.Errorf("receipt argument is missing")
	}

	ctxb := context.Background()
	resp, err := client.ValidateReceipt(ctxb, &crpc.ValidateReceiptRequest{
		Asset:   asset,
		Media:   media,
		Amount:  amount,
		Receipt: receipt,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}

var balanceCommand = cli.Command{
	Name:     "balance",
	Category: "Balance",
	Usage:    "Return asset balance.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "asset",
			Usage: "(optional) Asset is an acronym of the crypto currency",
		},
		cli.StringFlag{
			Name: "media",
			Usage: "(optional) Media is a type of technology which is used to" +
				" transport value of underlying asset",
		},
	},
	Action: balance,
}

func balance(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var (
		media crpc.Media
		asset crpc.Asset
	)

	switch {
	case ctx.IsSet("media"):
		stringMedia := ctx.String("media")
		switch stringMedia {
		case "blockchain":
			media = crpc.Media_BLOCKCHAIN
		case "lightning":
			media = crpc.Media_LIGHTNING
		default:
			return errors.Errorf("invalid media type %v, support media type "+
				"are: 'blockchain' and 'lightning'", stringMedia)
		}
	}

	switch {
	case ctx.IsSet("asset"):
		stringAsset := strings.ToLower(ctx.String("asset"))
		switch stringAsset {
		case "btc", "bitcoin":
			asset = crpc.Asset_BTC
		case "bch", "bitcoincash":
			asset = crpc.Asset_BCH
		case "ltc", "litecoin":
			asset = crpc.Asset_LTC
		case "eth", "ethereum":
			asset = crpc.Asset_ETH
		case "dash":
			asset = crpc.Asset_DASH
		default:
			return errors.Errorf("invalid asset %v, supported assets"+
				"are: 'btc', 'bch', 'dash', 'eth', 'ltc'", stringAsset)
		}
	}

	ctxb := context.Background()
	resp, err := client.Balance(ctxb, &crpc.BalanceRequest{
		Asset: asset,
		Media: media,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}

var estimateFeeCommand = cli.Command{
	Name:     "estimatefee",
	Category: "Fee",
	Usage:    "Estimates fee of the payment.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "asset",
			Usage: "Asset is an acronym of the crypto currency",
		},
		cli.StringFlag{
			Name: "media",
			Usage: "Media is a type of technology which is used to transport" +
				" value of underlying asset",
		},
		cli.StringFlag{
			Name: "amount",
			Usage: "(optional) Amount is the amount which will be sent by" +
				" service.",
		},
		cli.StringFlag{
			Name: "receipt",
			Usage: "(optional) Receipt represent either blockchains address" +
				" or lightning network invoice. If receipt is specified the " +
				"number are more accurate for lightning network media",
		},
	},
	Action: estimateFee,
}

func estimateFee(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var (
		media   crpc.Media
		asset   crpc.Asset
		amount  string
		receipt string
	)

	switch {
	case ctx.IsSet("media"):
		stringMedia := ctx.String("media")
		switch stringMedia {
		case "bl", "blockchain":
			media = crpc.Media_BLOCKCHAIN
		case "li", "lightning":
			media = crpc.Media_LIGHTNING
		default:
			return errors.Errorf("invalid media type %v, support media type "+
				"are: 'blockchain' and 'lightning'", stringMedia)
		}
	default:
		return errors.New("media argument missing")
	}

	switch {
	case ctx.IsSet("asset"):
		stringAsset := strings.ToLower(ctx.String("asset"))
		switch stringAsset {
		case "btc", "bitcoin":
			asset = crpc.Asset_BTC
		case "bch", "bitcoincash":
			asset = crpc.Asset_BCH
		case "ltc", "litecoin":
			asset = crpc.Asset_LTC
		case "eth", "ethereum":
			asset = crpc.Asset_ETH
		case "dash":
			asset = crpc.Asset_DASH
		default:
			return errors.Errorf("invalid asset %v, supported assets"+
				"are: 'btc', 'bch', 'dash', 'eth', 'ltc'", stringAsset)
		}
	default:
		return errors.Errorf("asset argument missing")
	}

	if ctx.IsSet("amount") {
		amount = ctx.String("amount")
	}

	if ctx.IsSet("receipt") {
		receipt = ctx.String("receipt")
	}

	ctxb := context.Background()
	resp, err := client.EstimateFee(ctxb, &crpc.EstimateFeeRequest{
		Asset:   asset,
		Media:   media,
		Amount:  amount,
		Receipt: receipt,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}

var sendPaymentCommand = cli.Command{
	Name:     "sendpayment",
	Category: "Payment",
	Usage:    "Sends payment",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "asset",
			Usage: "Asset is an acronym of the crypto currency",
		},
		cli.StringFlag{
			Name: "media",
			Usage: "Media is a type of technology which is used to transport" +
				" value of underlying asset",
		},
		cli.StringFlag{
			Name: "amount",
			Usage: "(optional) Amount is the amount which will be sent by" +
				" service.",
		},
		cli.StringFlag{
			Name: "receipt",
			Usage: "Receipt is either blockchain address or lightning network" +
				" invoice which identifies the receiver of the payment.",
		},
	},
	Action: sendPayment,
}

func sendPayment(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var (
		media   crpc.Media
		asset   crpc.Asset
		amount  string
		receipt string
	)

	switch {
	case ctx.IsSet("media"):
		stringMedia := ctx.String("media")
		switch stringMedia {
		case "bl", "blockchain":
			media = crpc.Media_BLOCKCHAIN
		case "li", "lightning":
			media = crpc.Media_LIGHTNING
		default:
			return errors.Errorf("invalid media type %v, support media type "+
				"are: 'blockchain' and 'lightning'", stringMedia)
		}
	default:
		return errors.New("media argument missing")
	}

	switch {
	case ctx.IsSet("asset"):
		stringAsset := strings.ToLower(ctx.String("asset"))
		switch stringAsset {
		case "btc", "bitcoin":
			asset = crpc.Asset_BTC
		case "bch", "bitcoincash":
			asset = crpc.Asset_BCH
		case "ltc", "litecoin":
			asset = crpc.Asset_LTC
		case "eth", "ethereum":
			asset = crpc.Asset_ETH
		case "dash":
			asset = crpc.Asset_DASH
		default:
			return errors.Errorf("invalid asset %v, supported assets"+
				"are: 'btc', 'bch', 'dash', 'eth', 'ltc'", stringAsset)
		}
	default:
		return errors.Errorf("asset argument missing")
	}

	if ctx.IsSet("amount") {
		amount = ctx.String("amount")
	} else if media == crpc.Media_BLOCKCHAIN {
		// In case of blockchain we always should specify amount.
		// In case of lighnting we might not do that if it specified in the
		// invoice.
		return errors.Errorf("amount argument is missing")
	}

	if ctx.IsSet("receipt") {
		receipt = ctx.String("receipt")
	} else {
		return errors.Errorf("receipt argument is missing")
	}

	ctxb := context.Background()
	resp, err := client.SendPayment(ctxb, &crpc.SendPaymentRequest{
		Asset:   asset,
		Media:   media,
		Amount:  amount,
		Receipt: receipt,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}

var paymentByIDCommand = cli.Command{
	Name:     "paymentbyid",
	Category: "Payment",
	Usage:    "Return payment by the given id",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name: "id",
			Usage: "ID it is unique identificator of the payment. " +
				"In case of blockchain media payment id is the transaction" +
				" id, in case of lightning media it is the payment hash.",
		},
	},
	Action: paymentByID,
}

func paymentByID(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var id string

	if ctx.IsSet("id") {
		id = ctx.String("id")
	} else {
		return errors.Errorf("id argument is missing")
	}

	ctxb := context.Background()
	resp, err := client.PaymentByID(ctxb, &crpc.PaymentByIDRequest{
		PaymentId: id,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}

var paymentByReceiptCommand = cli.Command{
	Name:     "paymentbyreceipt",
	Category: "Payment",
	Usage:    "Return payment by the given receipt",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name: "receipt",
			Usage: "Receipt is either blockchain address or lightning network" +
				" invoice which identifies the receiver of the payment.",
		},
	},
	Action: paymentByReceipt,
}

func paymentByReceipt(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var receipt string

	if ctx.IsSet("receipt") {
		receipt = ctx.String("receipt")
	} else {
		return errors.Errorf("receipt argument is missing")
	}

	ctxb := context.Background()
	resp, err := client.PaymentsByReceipt(ctxb, &crpc.PaymentsByReceiptRequest{
		Receipt: receipt,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}

var listPaymentsCommand = cli.Command{
	Name:     "listpayments",
	Category: "Payment",
	Usage:    "Return list payments by the given filter parameters",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "asset",
			Usage: "Asset is an acronym of the crypto currency",
		},
		cli.StringFlag{
			Name: "media",
			Usage: "Media is a type of technology which is used to transport" +
				" value of underlying asset",
		},
		cli.StringFlag{
			Name: "direction",
			Usage: "Direction identifies the direction of the payment, " +
				"(incoming, outgoing).",
		},
		cli.StringFlag{
			Name: "status",
			Usage: "Status is the state of the payment, " +
				"(waiting, pending, completed, failed).",
		},
		cli.StringFlag{
			Name: "system",
			Usage: "System denotes is that payment belongs to business logic" +
				" of payment server or it was originated by " +
				"user / third-party service (internal, external).",
		},
	},
	Action: listPayments,
}

func listPayments(ctx *cli.Context) error {
	client, cleanUp := getClient(ctx)
	defer cleanUp()

	var (
		media     crpc.Media
		asset     crpc.Asset
		status    crpc.PaymentStatus
		direction crpc.PaymentDirection
		system    crpc.PaymentSystem
	)

	if ctx.IsSet("media") {
		stringMedia := ctx.String("media")
		switch stringMedia {
		case "bl", "blockchain":
			media = crpc.Media_BLOCKCHAIN
		case "li", "lightning":
			media = crpc.Media_LIGHTNING
		default:
			return errors.Errorf("invalid media type %v, support media type "+
				"are: 'blockchain' and 'lightning'", stringMedia)
		}
	}

	if ctx.IsSet("asset") {
		stringAsset := strings.ToLower(ctx.String("asset"))
		switch stringAsset {
		case "btc", "bitcoin":
			asset = crpc.Asset_BTC
		case "bch", "bitcoincash":
			asset = crpc.Asset_BCH
		case "ltc", "litecoin":
			asset = crpc.Asset_LTC
		case "eth", "ethereum":
			asset = crpc.Asset_ETH
		case "dash":
			asset = crpc.Asset_DASH
		default:
			return errors.Errorf("invalid asset %v, supported assets"+
				"are: 'btc', 'bch', 'dash', 'eth', 'ltc'", stringAsset)
		}
	}

	if ctx.IsSet("status") {
		stringStatus := strings.ToLower(ctx.String("status"))
		switch stringStatus {
		case strings.ToLower(crpc.PaymentStatus_WAITING.String()):
			status = crpc.PaymentStatus_WAITING

		case strings.ToLower(crpc.PaymentStatus_PENDING.String()):
			status = crpc.PaymentStatus_PENDING

		case strings.ToLower(crpc.PaymentStatus_COMPLETED.String()):
			status = crpc.PaymentStatus_COMPLETED

		case strings.ToLower(crpc.PaymentStatus_FAILED.String()):
			status = crpc.PaymentStatus_FAILED
		default:
			return errors.Errorf("invalid status %v, supported statuses"+
				"are: 'waiting', 'pending', 'completed', 'failed'",
				stringStatus)
		}
	}

	if ctx.IsSet("direction") {
		stringDirection := strings.ToLower(ctx.String("direction"))
		switch stringDirection {

		case strings.ToLower(crpc.PaymentDirection_OUTGOING.String()):
			direction = crpc.PaymentDirection_OUTGOING

		case strings.ToLower(crpc.PaymentDirection_INCOMING.String()):
			direction = crpc.PaymentDirection_INCOMING

		default:
			return errors.Errorf("invalid direction %v, supported direction"+
				"are: 'incoming', 'outgoing'",
				stringDirection)
		}
	}

	if ctx.IsSet("system") {
		stringSystem := strings.ToLower(ctx.String("system"))
		switch stringSystem {
		case strings.ToLower(crpc.PaymentSystem_INTERNAL.String()):
			system = crpc.PaymentSystem_INTERNAL

		case strings.ToLower(crpc.PaymentSystem_EXTERNAL.String()):
			system = crpc.PaymentSystem_EXTERNAL

		default:
			return errors.Errorf("invalid system %v, supported system"+
				"are: 'internal', 'external'",
				stringSystem)
		}
	}

	ctxb := context.Background()
	resp, err := client.ListPayments(ctxb, &crpc.ListPaymentsRequest{
		Status:    status,
		Direction: direction,
		Asset:     asset,
		Media:     media,
		System:    system,
	})
	if err != nil {
		return err
	}

	printRespJSON(resp)
	return nil
}
const getRandomValues = (buf) => {
  if (typeof process !== 'undefined') {
    const nodeCrypto = require('crypto');
    const bytes = nodeCrypto.randomBytes(buf.length);
    buf.set(bytes);
    return buf;
  }

  if (window.crypto && window.crypto.getRandomValues) {
    return window.crypto.getRandomValues(buf);
  }

  if (window.msCrypto && window.msCrypto.getRandomValues) {
    return window.msCrypto.getRandomValues(buf);
  }

  throw new Error('No secure random number generator available.');
};

module.exports = getRandomValues;
#!/bin/bash
set -e

# Colour constants
bold=`tput bold`
green=`tput setaf 2`
red=`tput setaf 1`
reset=`tput sgr0`

ALICE_PORT=10001
BOB_PORT=10002

ALICE_LOG=bin/testnet/test/alice.txt
BOB_LOG=bin/testnet/test/bob.txt

if test -d bin; then cd bin; fi

echo "${bold}Mounting a RAM disk for server output in test directory!${reset}"
if mountpoint -q -- "test"; then
    sudo umount test
fi

rm -r test | true # in case this is the first time being run
mkdir test && sudo mount -t tmpfs -o size=5000m tmpfs test

# Source Intel Libraries
source /opt/intel/sgxsdk/environment

pushd ../../ # go to source directory
echo "${bold}Starting two ghost teechain enclaves...${reset}"

echo "${bold}Spawning enclave ALICE listening on port $ALICE_PORT in $ALICE_LOG ${reset}"
./teechain ghost -d -p $ALICE_PORT > $ALICE_LOG 2>&1 &
sleep 1

echo "${bold}Spawning enclave BOB listening on port $BOB_PORT in $BOB_LOG ${reset}"
./teechain ghost -d -p $BOB_PORT > $BOB_LOG 2>&1 &
sleep 1

echo -n "${red}Waiting until enclaves are initialized ...!${reset}"
for u in alice bob; do  #TODO: generalize to multiple parties (not just 4)
    while [ "$(grep -a 'Enclave created' bin/testnet/test/${u}.txt | wc -l)" -eq 0 ]; do
        sleep 0.1
        echo -n "."
    done
done

# Create primaries
./teechain primary -p $ALICE_PORT
./teechain primary -p $BOB_PORT

# Setup up primaries with number of deposits
./teechain setup_deposits 5 -p $ALICE_PORT
./teechain setup_deposits 3 -p $BOB_PORT

# Deposits made
./teechain deposits_made mmY6ijr6uLP3DdRFC4nwL23HSKsH2xgy74 1 5 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feA 0 1 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feA 1 1 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feA 2 1 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feA 3 1 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feA 4 1 -p $ALICE_PORT
./teechain deposits_made my6NJU1T6gL5f3TfmSPN4idUytdCQHTmsU 1 3 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feB 0 1 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feB 1 1 edec34c9bb3a4395cd8d1e9300725f537235d8a058fc6a7ae519003b64fd0feB 2 1  -p $BOB_PORT

# Create and establish a channel between Alice and Bob
./teechain create_channel -p $BOB_PORT &
sleep 1
./teechain create_channel -i -r 127.0.0.1:$BOB_PORT -p $ALICE_PORT # Initiator

sleep 2

# Extract the channel id for the channel created
CHANNEL_1=$(grep "Channel ID:" $ALICE_LOG | awk '{print $3}')

# Verified the setup transactions are in the blockchain
./teechain verify_deposits $CHANNEL_1 -p $BOB_PORT &
./teechain verify_deposits $CHANNEL_1 -p $ALICE_PORT

sleep 2

# Alice and Bob add deposits to their channels now
./teechain add_deposit $CHANNEL_1 0 -p $ALICE_PORT
./teechain add_deposit $CHANNEL_1 0 -p $BOB_PORT

# Alice check balance matches expected
./teechain balance $CHANNEL_1 -p $ALICE_PORT
if ! tail -n 2 $ALICE_LOG | grep -q "My balance is: 1, remote balance is: 1"; then
    echo "Alice's balance check failed on channel setup!"; exit 1;
fi

# Send from Bob to Alice
./teechain send $CHANNEL_1 1 -p $BOB_PORT

# Alice check balance after
./teechain balance $CHANNEL_1 -p $ALICE_PORT
if ! tail -n 2 $ALICE_LOG | grep -q "My balance is: 2, remote balance is: 0"; then
    echo "Alice's balance check failed after send!"; exit 1;
fi

# Send from Bob to Alice should fail. Bob check balance, shouldn't have changed
./teechain send $CHANNEL_1 1 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 0, remote balance is: 2"; then
    echo "Bob's balance check failed!"; exit 1;
fi
# Add deposit from bob's side and check balance
./teechain add_deposit $CHANNEL_1 1 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 1, remote balance is: 2"; then
    echo "Bob's balance check failed!"; exit 1;
fi
echo "Bob added another deposit to his channel!"
# Send from Bob to Alice and check balance is back to zero
./teechain send $CHANNEL_1 1 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 0, remote balance is: 3"; then
    echo "Bob's balance check failed!"; exit 1;
fi
# Send from Alice to Bob and check Bob's balance on Alice's side
./teechain send $CHANNEL_1 1 -p $ALICE_PORT
./teechain balance $CHANNEL_1 -p $ALICE_PORT
if ! tail -n 2 $ALICE_LOG | grep -q "My balance is: 2, remote balance is: 1"; then
    echo "Alice's balance check failed!"; exit 1;
fi
# Bob remove deposit and check balance
./teechain remove_deposit $CHANNEL_1 1 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 0, remote balance is: 2"; then
    echo "Bob's balance check failed!"; exit 1;
fi
echo "Bob removed the deposit from his channel!"
# Bob try to remove first deposit, should fail as insufficient funds
./teechain remove_deposit $CHANNEL_1 0 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 0, remote balance is: 2"; then
    echo "Bob's balance check failed!"; exit 1;
fi
echo "Bob removed his last deposit from the channel!"
# Bob now send 1 to alice
./teechain send $CHANNEL_1 1 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 0, remote balance is: 1"; then
    echo "Bob's balance check failed!"; exit 1;
fi
echo "Bob sent 1 to Alice!"
# Alice removed last deposit from channel
./teechain remove_deposit $CHANNEL_1 0 -p $ALICE_PORT
./teechain balance $CHANNEL_1 -p $ALICE_PORT
if ! tail -n 2 $ALICE_LOG | grep -q "My balance is: 0, remote balance is: 0"; then
    echo "Alice's balance check failed!"; exit 1;
fi
echo "Alice removed her last deposit from the channel!"
# Bob now send 1 to alice
./teechain send $CHANNEL_1 1 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 0, remote balance is: 0"; then
    echo "Bob's balance check failed!"; exit 1;
fi
echo "Bob tried to send 1 to alice, but it didnt work!"
# Add all the deposits to the channel (both sides)
./teechain add_deposit $CHANNEL_1 0 -p $ALICE_PORT
./teechain add_deposit $CHANNEL_1 1 -p $ALICE_PORT
./teechain add_deposit $CHANNEL_1 2 -p $ALICE_PORT
./teechain add_deposit $CHANNEL_1 3 -p $ALICE_PORT
./teechain add_deposit $CHANNEL_1 4 -p $ALICE_PORT
./teechain add_deposit $CHANNEL_1 0 -p $BOB_PORT
./teechain add_deposit $CHANNEL_1 1 -p $BOB_PORT
./teechain add_deposit $CHANNEL_1 2 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $ALICE_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $ALICE_LOG | grep -q "My balance is: 5, remote balance is: 3"; then
    echo "Alice's balance check failed!"; exit 1;
fi
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 3, remote balance is: 5"; then
    echo "Bob's balance check failed!"; exit 1;
fi
echo "All deposits added to the channel!"
# Bob now send 3 to alice
./teechain send $CHANNEL_1 3 -p $BOB_PORT
./teechain balance $CHANNEL_1 -p $BOB_PORT
if ! tail -n 2 $BOB_LOG | grep -q "My balance is: 0, remote balance is: 8"; then
    echo "Bob's balance check failed!"; exit 1;
fi
echo "Bob sent all 3 to Alice!"
# Settle and shutdown
./teechain settle_channel $CHANNEL_1 -p $ALICE_PORT
# Alice decides to get her unused deposits out (there are no used deposits!)
./teechain shutdown -p $ALICE_PORT
popd # return to bin directory
../kill.sh
echo "${bold}Looks like the test passed!${reset}"
  -----BEGIN CERTIFICATE-----
MIIDhTCCAm2gAwIBAgIJALjCgEBIwDscMA0GCSqGSIb3DQEBBQUAMFkxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMMCWxvY2FsaG9zdDAeFw0xMzAzMDgxMzQw
MDJaFw0yMzAzMDYxMzQwMDJaMFkxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21l
LVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNV
BAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOTL
p47Qy1hovBC6VWi33CCpq5r5+QHnt5PLsjhOoZ0VjHI0KYNMPkT9yfwJZO8vHEsW
dDoW+fRojp+VO6JOYcO1JAr0jBlnzfOlr+zBHKvaEWylku9DS5ZbxLnj4AQe5m5/
uqtlQt4ib4vXQr3yfW8B9Jy55OfWV8m9orfxubOzK1Ll0LeDwubKgUwuzB3auJKb
VNsIlZQzrKDzMoTExtKF/7cSUC+5+1UHFy9rUh9VOtU2RkFJQgOPOyw9lmg7pCfl
uurz4Q8wjSchhWvMnEc8YenqOaA+AcmlFiHwQq3z0aILCa5IEUOUzwER4bZM6eDe
8rZLG+uRAABhhfC/LfUCAwEAAaNQME4wHQYDVR0OBBYEFEhAKuSwT9BxLaHcxzmn
CDZ7bxycMB8GA1UdIwQYMBaAFEhAKuSwT9BxLaHcxzmnCDZ7bxycMAwGA1UdEwQF
MAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAIK1pI70uzMET8QAJ6o0rBsuYnat9WeL
Y/27yKWg440BoWYxI6XJPP+nnncesGxcElnQCPkut8ODIGG037WKuQNRMr7dBQeU
MIaIxKGZETGIj5biao6tqYuWwIS54NxOTIUVx4QomXnyLNyE0Mj4ftD8bKEIuVfV
2bDC6UjN02lPh2IsV+th5oOr3BShwafu+7CAKLSaidraUW/hGKSWpMgBSBHnA2tD
W3mLidFxB2ufi6ufT87HliC6AJw6S9A5+iuAIEuRGV4zhc4BZpKTeeFRVWYPUBtp
/SoNIeLQ4ORhIFQjTY2nEq2lGnCJ0JcTTt1gVNbsEitRtw0eAUtMTXs=
-----END CERTIFICATE-----
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAuZ4RZVnjh8kS32TZn7pMWJevf473pLqecObWMLmeB6FIzpvf
kYi8RyLD9Q87QjmIhbrqFncyaBaw1iz5sx2sVM2+acwlocN4UHPBCxwvmtUeEn1t
WMp29D4NetJNssvq7PxzcXY7bv7FQ7q7kJ5KpoBc+OBu/4vdQhM0gkR5QEL52KNj
C8umfhc2aEeRn4et9lBqNlE4WjH3s5tOO3DqNt5kQ28hulcSaiPMaKPcjqmAYz8+
VLOY7fefGNBecr72MaA5St+oc0TDK6msHPoTtYe4b6r6AsyM9O+7f82idtWK9nu/
rjQZP2UeMQvqUtQj+Ar3WoM60SkEQ8Ckq6PQPwIDAQABAoIBAFUlZFd4r34HR8gA
LDAwNnthQZkjDQicrredvF1nmE1pt8tHB3xsG+oJ0Zgln4pWeADYaC9cCFxlJENr
KDP5Bad1JcbEZfLZhuRo5QHisRe2cXAL51AWuBB8MpTHyeqdzitd9tryYHsfFYBn
NUk2w4mzUnK8CU7iauG3i5vCK1jFV9OvedeQGjmKcJ39U4R8qOQesTP1x0tc7C8Y
SgSNaicZKXcHOlHntk6sGfpCekDX0bPKAOB2CMtbujeUNB/wgM/eEGLugdddXHfV
GErnqqnSCUog3bhZLaEOdl4XOJZtBmKIzQcUecNH3myADgpSm+AethCYErRqmvIj
FhXNfVkCgYEA7B2NjuOeaxoIgqPVN+DFVOVOv25AODLpgZDrfPZqr0E231LaexRn
xtsuJpxQ/lGPgY6dOrhX6d5HEQ2JrFDiKyv+uP7V/s9Wp562UhSMRLzuXWg7phto
yuia2bwj9k4Fwl9b3tQfJMxUulv2Bkq4+ZtuX0bFw8P4C3xwQMLQCgMCgYEAyT/S
UFIFp2u/7tXce5qrT3Z/SMe3qa+1DeosmhdCNQdJhV0p7rARX0bl+UN1oeDg65Sb
khzmTf+zpB0Nho63+W/CjlSdHBBFPTgSgjejkfiENfW63HBT18K0ya2LC4+fOuWg
e35VBJjKZT4nUTjZ/rscdeKNve4SvSWl3dFPqhUCgYEAgqIbJroydKkTmkvQdLjs
FHtF5U23RCCO5ntqflnLTqkzFb2+WShB/lhXoU8M1JgFYLWpsco6AY9UHFA0Il0h
tKcDqBB+Dxthox2BW8o4jPNGofFARzeU8+ZbfinEb8pdD1w49QDBNlfCbNTiOjrv
OlJPb3E1i4kJ3Dj91iayeUcCgYEAgS5qfgxofLN5nIHC6cS6umNCCSHKDy4udiQf
RTow0YE//E91HzX9sL792CcpVyPWvOHDiuLqIp9EXNAZYooyJfdLV7mQr/bxuv5H
Qzcb1BNGKqz1qZKg/xqImfzACEfE2jWT8jGBuVWqdZqT+lsX85+AAVvPyF8NwERu
WBiHnpECgYA28LMcfOVplez7z7wxzoVZyq7I7yV50RCxZiJ6GepZPzTnqR2LAmb6
2qMOJkShHk/pydtF+49j9/MjWJexGWaCbsFaei/bnsZfskEF+/2MFmBp6fAN1FRP
FLNEF+YTPz6yFCNWecZ2INEAokEi2D809XhDQwiJz0E2vEzhR93fDg==
-----END RSA PRIVATE KEY-----
  
© 2019 GitHub, Inc.
