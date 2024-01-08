package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const (
	LABEL                         = "content"
	PUBLIC_ENCRYPTED_KEY_LOCATION = "./keys/encryption-pub.pem"
	PUBLIC_AUTH_KEY_LOCATION      = "./keys/id_rsa.pub"
	PRIVATE_AUTH_KEY_LOCATION     = "./keys/id_rsa"
	USER                          = "foo"
)

func ObtainKeyBytes(keyLoc string) ([]byte, error) {
	// Not part of the resource utilization, no need for locking.
	data, err := os.ReadFile(keyLoc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func ObtainSshAuth(privateKeyLoc string) (ssh.AuthMethod, error) {
	data, err := ObtainKeyBytes(privateKeyLoc)
	if err != nil {
		return nil, err
	}
	key, err := ssh.ParsePrivateKey(data)
	if err != nil {
		return nil, err
	}

	return ssh.PublicKeys(key), nil
}

func FetchPubKey(Loc string) (*rsa.PublicKey, error) {
	data, err := ObtainKeyBytes(Loc)
	if err != nil {
		return nil, err
	}

	pem, _ := pem.Decode(data)
	if pem == nil {
		return nil, errors.New("failed to extract RSA public key from PEM block")
	}

	parse, err := x509.ParsePKIXPublicKey(pem.Bytes)
	if err != nil {
		return nil, err
	}
	pubKey, ok := parse.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("unable to parse key into RSA key.")
	}
	return pubKey, nil
}

func EncryptData(content []byte) ([]byte, error) {
	pubKey, err := FetchPubKey(PUBLIC_ENCRYPTED_KEY_LOCATION)
	if err != nil {
		return nil, err
	}
	rand := rand.Reader
	return rsa.EncryptOAEP(sha256.New(), rand, pubKey, content, []byte(LABEL))
}

func TransmitFile(addr string, fileName string, fileData []byte) error {

	keyData, err := ObtainKeyBytes(PUBLIC_AUTH_KEY_LOCATION)
	if err != nil {
		return err
	}

	pubKey, _, _, _, err := ssh.ParseAuthorizedKey(keyData)
	if err != nil {
		return err
	}

	publicKey, err := ssh.ParsePublicKey(pubKey.Marshal())
	if err != nil {
		return err
	}

	authenticationMethod, err := ObtainSshAuth(PRIVATE_AUTH_KEY_LOCATION)
	if err != nil {
		return err
	}

	configuration := ssh.ClientConfig{
		User:            USER,
		Auth:            []ssh.AuthMethod{authenticationMethod},
		HostKeyCallback: ssh.FixedHostKey(publicKey),
	}

	con, err := ssh.Dial("tcp", string(addr), &configuration)
	if err != nil {
		log.Printf("Failed to dial %v, error: %v\n", string(addr), err)
		return err
	}
	log.Printf("Connected to %v\n", string(addr))

	con.SendRequest("keepalive", false, nil)

	client, err := sftp.NewClient(con)
	if err != nil {
		log.Printf("Failed to create client %v, error: %v\n", string(addr), err)
		return err
	}

	fileLoc := filepath.Join("resources", fileName)

	errMkdir := client.MkdirAll("resources")
	if errMkdir != nil {
		log.Printf("Failed to create directory, error: %v\n", errMkdir)
		return errMkdir
	}

	file, err := client.Create(fileLoc)
	if err != nil {
		log.Printf("Failed to create file %v, error: %v\n", fileName, err)
		return err
	}

	_, errWrt := file.ReadFrom(bytes.NewReader(fileData))
	if errWrt != nil {
		log.Printf("Failed to create file %v, error: %v\n", fileName, err)
		return errWrt
	}

	file.Close()
	client.Close()

	return nil
}
