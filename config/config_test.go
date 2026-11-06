package config

import (
	"fmt"
	"os"
	"testing"

	"Tuna_v2/crypto"
)

func inTempDir(t *testing.T) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Error(err)
		}
	})
}

func TestGenerateKeys(t *testing.T) {
	inTempDir(t)
	GenerateKeyFiles(4, "./")
}

func TestGenrateTsKeys(t *testing.T) {
	inTempDir(t)
	GenerateTsKeyFiles(4, 3, "./")
}

func TestFromFileGenKey(t *testing.T) {
	inTempDir(t)
	GenerateKeyFiles(1, "./")
	filename := "./.node-key-0.json"
	pub, pri, err := GenKeysFromFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	srvc := crypto.NewSigService(pri, crypto.SecretShareKey{})
	d := crypto.NewHasher().Sum256([]byte("dcz"))
	sig, _ := srvc.RequestSignature(d)
	if !sig.Verify(pub, d) {
		t.Fatalf("error")
	}
}

func TestFromFileGenTsKey(t *testing.T) {
	inTempDir(t)
	GenerateTsKeyFiles(4, 3, "./")

	var shareKeys []crypto.SecretShareKey

	for i := 0; i < 4; i++ {
		filename := fmt.Sprintf("./.node-ts-key-%d.json", i)
		shareKey, err := GenTsKeyFromFile(filename)
		if err != nil {
			t.Fatal(err)
		}

		shareKeys = append(shareKeys, shareKey)
	}

	var (
		cnt        = 0
		shareSigch = make(chan crypto.SignatureShare, 4)
	)

	msg := []byte("dczhahahah")
	digest := crypto.NewHasher().Sum256(msg)

	for i := 0; i < 4; i++ {
		ind := i
		// 1. Decode/Encode
		byt, err := crypto.EncodeTSPartialKey(shareKeys[ind].PriShare)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("[%d] share %s\n", ind, byt)

		share, err := crypto.DecodeTSPartialKey(byt)
		if err != nil {
			t.Fatal(err)
		}

		if share.String() != shareKeys[ind].PriShare.String() {
			t.Fatal("encode/decode error")
		}

		// 2. Sign
		srvc := crypto.NewSigService(crypto.PrivateKey{}, shareKeys[ind])
		sigShare, err := srvc.RequestTsSugnature(digest)
		if err != nil {
			t.Fatal(err)
		}

		shareSigch <- sigShare
	}

	var sigs []crypto.SignatureShare

	for sig := range shareSigch {
		sigs = append(sigs, sig)
		cnt++
		if cnt == 3 {
			break
		}
	}

	combineSig, err := crypto.CombineIntactTSPartial(sigs, shareKeys[0], digest)
	if err != nil {
		t.Fatal(err)
	}

	if err := crypto.VerifyTs(shareKeys[0], digest, combineSig); err != nil {
		t.Fatal(err)
	}
}

func TestSmapleParameters(t *testing.T) {
	inTempDir(t)
	GenerateSampleParameters()
}

func TestSmapleCommittee(t *testing.T) {
	inTempDir(t)
	GenerateSmapleCommittee()
}

func TestGenParametersFromFile(t *testing.T) {
	inTempDir(t)
	GenerateSampleParameters()
	poolP, mempoolP, err := GenParamatersFromFile("./.parameters.json")
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(poolP)
		t.Log(mempoolP)
		if poolP != GenDefaultParameters().Pool || mempoolP != GenDefaultParameters().Mempool {
			t.Fatal("parameter round trip changed infrastructure settings")
		}
	}
}

func TestGenCommitteeFromFile(t *testing.T) {
	inTempDir(t)
	GenerateSmapleCommittee()
	committee, err := GenCommitteeFromFile("./.committee.json")
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(committee)
		if committee.Size() != 4 {
			t.Fatal("incorrect committee size")
		}

		for _, authority := range committee.Authorities {
			if authority.Addr == "" || authority.MempoolAddr == "" {
				t.Fatal("missing node address")
			}
		}
	}
}
