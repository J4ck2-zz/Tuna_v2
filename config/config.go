package config

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/dag"
	"Tuna_v2/mempool"
	"Tuna_v2/pool"
)

// savetoFile save data to filename in json style
func savetoFile(filename string, data interface{}) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		panic(err)
	}

	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")
	if err := encoder.Encode(data); err != nil {
		panic(err)
	}
}

// GenerateKeyFiles generate keys file
func GenerateKeyFiles(pairs int, path string) {
	for i := 0; i < pairs; i++ {
		filename := fmt.Sprintf("%s/.node-key-%d.json", path, i)
		keys := make(map[string]interface{})
		pri, pub := crypto.GenED25519Keys()
		keys["public"] = string(crypto.EncodePublicKey(crypto.PublickKey{
			Pubkey: pub,
		}))
		keys["private"] = string(crypto.EncodePrivateKey(crypto.PrivateKey{
			Prikey: pri,
		}))
		savetoFile(filename, keys)
	}
}

// GenerateTsKeyFiles generate ts keys file
func GenerateTsKeyFiles(N, T int, path string) {
	shares, pub := crypto.GenTSKeys(T, N)
	for i := 0; i < N; i++ {
		filename := fmt.Sprintf("%s/.node-ts-key-%d.json", path, i)
		keys := make(map[string]interface{})
		share, _ := crypto.EncodeTSPartialKey(shares[i])
		pub, _ := crypto.EncodeTSPublicKey(pub)
		keys["share"] = string(share)
		keys["pub"] = string(pub)
		keys["N"] = N
		keys["T"] = T
		savetoFile(filename, keys)
	}
}

// readFromFile read json file
func readFromFile(filename string) (map[string]interface{}, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		return nil, err
	}

	defer file.Close()
	decode := json.NewDecoder(file)
	data := make(map[string]interface{})
	if err := decode.Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

// GenKeysFromFile generate keys from json file
func GenKeysFromFile(filename string) (crypto.PublickKey, crypto.PrivateKey, error) {
	var entry struct {
		Public  string `json:"public"`
		Private string `json:"private"`
	}

	file, err := os.Open(filename)
	if err != nil {
		return crypto.PublickKey{}, crypto.PrivateKey{}, err
	}

	defer file.Close()
	if err := json.NewDecoder(file).Decode(&entry); err != nil {
		return crypto.PublickKey{}, crypto.PrivateKey{}, err
	}

	public, err := crypto.DecodePublicKey([]byte(entry.Public))
	if err != nil {
		return public, crypto.PrivateKey{}, err
	}

	private, err := crypto.DecodePrivateKey([]byte(entry.Private))
	if err != nil {
		return public, private, err
	}

	if len(public.Pubkey) != ed25519.PublicKeySize ||
		len(private.Prikey) != ed25519.PrivateKeySize ||
		!public.Pubkey.Equal(private.Prikey.Public()) {
		return public, private, fmt.Errorf("invalid or mismatched Ed25519 key pair")
	}

	return public, private, nil
}

// GenTsKeyFromFile generate ts keys from json file
func GenTsKeyFromFile(filename string) (crypto.SecretShareKey, error) {
	if data, err := readFromFile(filename); err != nil {
		return crypto.SecretShareKey{}, err
	} else {
		share := data["share"].(string)
		pub := data["pub"].(string)
		N := data["N"].(float64)
		T := data["T"].(float64)
		shareKey := crypto.SecretShareKey{
			N: int(N),
			T: int(T),
		}
		shareKey.PriShare, err = crypto.DecodeTSPartialKey([]byte(share))
		if err != nil {
			return crypto.SecretShareKey{}, err
		}

		shareKey.PubPoly, err = crypto.DecodeTSPublicKey([]byte(pub))
		if err != nil {
			return crypto.SecretShareKey{}, err
		}

		return shareKey, nil
	}
}

// LoadParameters fills omitted fields from defaults and rejects unknown settings.
func LoadParameters(filename string) (Parameters, error) {
	parameters := GenDefaultParameters()
	file, err := os.Open(filename)
	if err != nil {
		return parameters, err
	}

	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&parameters)
	return parameters, err
}

// GenParamatersFromFile retains the existing infrastructure-only accessor.
func GenParamatersFromFile(filename string) (pool.Parameters, mempool.Parameters, error) {
	parameters, err := LoadParameters(filename)
	return parameters.Pool, parameters.Mempool, err
}

// GenCommitteeFromFile generate committee from json file
func GenCommitteeFromFile(filename string) (core.Committee, error) {
	var entries map[string]struct {
		Name           string `json:"name"`
		ID             int    `json:"node_id"`
		Address        string `json:"addr"`
		MempoolAddress string `json:"mempool_addr"`
	}

	committee := core.Committee{
		Authorities: make(map[core.NodeID]core.Authority),
	}
	file, err := os.Open(filename)
	if err != nil {
		return committee, err
	}

	defer file.Close()
	if err := json.NewDecoder(file).Decode(&entries); err != nil {
		return committee, err
	}

	addresses := make(map[string]bool)
	for key, entry := range entries {
		id, err := strconv.Atoi(key)
		if err != nil || id < 0 || entry.ID != id {
			return committee, fmt.Errorf("invalid committee node ID %q", key)
		}

		public, err := crypto.DecodePublicKey([]byte(entry.Name))
		if err != nil || len(public.Pubkey) != ed25519.PublicKeySize {
			return committee, fmt.Errorf("invalid public key for node %d", id)
		}

		for _, address := range []string{entry.Address, entry.MempoolAddress} {
			_, port, err := net.SplitHostPort(address)
			number, parseErr := strconv.Atoi(port)
			if err != nil || parseErr != nil || number < 1 || number > 65535 || addresses[address] {
				return committee, fmt.Errorf("invalid or duplicate address %q", address)
			}

			addresses[address] = true
		}

		committee.Authorities[core.NodeID(id)] = core.Authority{
			Id:          core.NodeID(id),
			Name:        public,
			Addr:        entry.Address,
			MempoolAddr: entry.MempoolAddress,
		}
	}

	return committee, nil
}

func GenerateSmapleCommittee() {
	committee, _ := GenDefaultCommittee(4)
	data := make(map[string]interface{})
	for id, item := range committee.Authorities {
		data[strconv.Itoa(int(id))] = map[string]interface{}{
			"name":         string(crypto.EncodePublicKey(item.Name)),
			"node_id":      id,
			"addr":         item.Addr,
			"mempool_addr": item.MempoolAddr,
		}
	}

	savetoFile("./.committee.json", data)
}

// GenerateSampleParameters generate parameters sample file
func GenerateSampleParameters() {
	parameters := GenDefaultParameters()
	savetoFile("./.parameters.json", parameters)
}

type Parameters struct {
	Consensus dag.Parameters     `json:"consensus"`
	Pool      pool.Parameters    `json:"pool"`
	Mempool   mempool.Parameters `json:"mempool"`
}

const DefaultPort int = 9000

// GenDefaultParameters generate default parameters
func GenDefaultParameters() Parameters {
	return Parameters{
		Consensus: dag.DefaultParameters,
		Pool:      pool.DefaultParameters,
		Mempool:   mempool.DefaultParameters,
	}
}

// GenDefaultCommittee generates node identities and addresses without a quorum policy.
func GenDefaultCommittee(n int) (core.Committee, []crypto.PrivateKey) {
	committee := core.Committee{
		Authorities: make(map[core.NodeID]core.Authority),
	}
	priKeys := make([]crypto.PrivateKey, 0, n)
	for i := 0; i < n; i++ {
		priKey, pubKey := crypto.GenED25519Keys()
		committee.Authorities[core.NodeID(i)] = core.Authority{
			Name: crypto.PublickKey{
				Pubkey: pubKey,
			},
			Id:          core.NodeID(i),
			Addr:        fmt.Sprintf("127.0.0.1:%d", DefaultPort+i),
			MempoolAddr: fmt.Sprintf("127.0.0.1:%d", DefaultPort+n+i),
		}
		priKeys = append(priKeys, crypto.PrivateKey{
			Prikey: priKey,
		})
	}

	return committee, priKeys
}

// GenerateLocalFiles creates a runnable static committee and matching key files.
func GenerateLocalFiles(n, port int, path string) error {
	if n < 1 || n > 1000 || port < 1 || port+2*n-1 > 65535 {
		return fmt.Errorf("invalid local committee size or port range")
	}

	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}

	GenerateKeyFiles(n, path)
	members := make(map[string]any)
	for i := 0; i < n; i++ {
		public, _, err := GenKeysFromFile(filepath.Join(path, fmt.Sprintf(".node-key-%d.json", i)))
		if err != nil {
			return err
		}

		members[strconv.Itoa(i)] = map[string]any{
			"name":         string(crypto.EncodePublicKey(public)),
			"node_id":      i,
			"addr":         fmt.Sprintf("127.0.0.1:%d", port+i),
			"mempool_addr": fmt.Sprintf("127.0.0.1:%d", port+n+i),
		}
	}

	savetoFile(filepath.Join(path, "committee.json"), members)
	parameters := GenDefaultParameters()
	parameters.Consensus.LeaderCount = min(n, parameters.Consensus.LeaderCount)
	savetoFile(filepath.Join(path, "parameters.json"), parameters)
	return nil
}
