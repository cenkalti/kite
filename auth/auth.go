package auth

import (
	"github.com/koding/kite/config"
	"github.com/koding/kite/simple"
)

const (
	Version     = "0.0.1"
	DefaultPort = 3997
	TokenTTL    = 1 * time.Hour
	TokenLeeway = 1 * time.Minute
)

var log logging.Logger

type Auth struct {
	*simple.Simple
	publicKey  string // RSA key for validation of tokens
	privateKey string // RSA key for signing tokens
}

func New(conf *config.Config, publicKey, privateKey string) *Auth {
	k := simple.New("auth", Version)
	k.Config = conf

	// Listen on default port if not set
	if k.Config.Port == 0 {
		k.Config.Port = DefaultPort
	}

	log = k.Log
	kontrol := &Kontrol{
		Simple:     k,
		publicKey:  publicKey,
		privateKey: privateKey,
	}

	k.HandleFunc("getToken", kontrol.handleGetToken)
	return kontrol
}

func (k *Kontrol) handleGetToken(r *kite.Request) (interface{}, error) {
	var query protocol.KontrolQuery
	err := r.Args.One().Unmarshal(&query)
	if err != nil {
		return nil, errors.New("Invalid query")
	}

	kiteKey, err := getQueryKey(&query)
	if err != nil {
		return nil, err
	}

	event, err := k.store.Get(
		KitesPrefix+kiteKey, // path
		false, // recursive
		false, // sorted
	)
	if err != nil {
		if err2, ok := err.(*etcdErr.Error); ok && err2.ErrorCode == etcdErr.EcodeKeyNotFound {
			return nil, errors.New("Kite not found")
		}
		return nil, err
	}

	var kiteVal registerValue
	err = json.Unmarshal([]byte(event.Node.Value), &kiteVal)
	if err != nil {
		return nil, err
	}

	return generateToken(kiteKey, r.Username, k.Server.Kite.Kite().Username, k.privateKey)
}

func addTokenToKites(nodes store.NodeExterns, username, issuer, queryKey, privateKey string) ([]*protocol.KiteWithToken, error) {
	kitesWithToken := make([]*protocol.KiteWithToken, len(nodes))

	for i, node := range nodes {
		kite, err := kiteFromEtcdKV(node.Key)
		if err != nil {
			return nil, err
		}

		kitesWithToken[i], err = addTokenToKite(kite, username, issuer, queryKey, privateKey)
		if err != nil {
			return nil, err
		}

		rv := new(registerValue)
		json.Unmarshal([]byte(node.Value), rv)

		kitesWithToken[i].URL = rv.URL.String()
	}

	return kitesWithToken, nil
}

func addTokenToKite(kite *protocol.Kite, username, issuer, queryKey, privateKey string) (*protocol.KiteWithToken, error) {
	tkn, err := generateToken(queryKey, username, issuer, privateKey)
	if err != nil {
		return nil, err
	}

	return &protocol.KiteWithToken{
		Kite:  *kite,
		Token: tkn,
	}, nil
}

// generateToken returns a JWT token string. Please see the URL for details:
// http://tools.ietf.org/html/draft-ietf-oauth-json-web-token-13#section-4.1
func generateToken(queryKey string, username, issuer, privateKey string) (string, error) {
	tknID, err := uuid.NewV4()
	if err != nil {
		return "", errors.New("Server error: Cannot generate a token")
	}

	// Identifies the expiration time after which the JWT MUST NOT be accepted
	// for processing.
	ttl := TokenTTL

	// Implementers MAY provide for some small leeway, usually no more than
	// a few minutes, to account for clock skew.
	leeway := TokenLeeway

	tkn := jwt.New(jwt.GetSigningMethod("RS256"))
	tkn.Claims["iss"] = issuer                                       // Issuer
	tkn.Claims["sub"] = username                                     // Subject
	tkn.Claims["aud"] = queryKey                                     // Audience
	tkn.Claims["exp"] = time.Now().UTC().Add(ttl).Add(leeway).Unix() // Expiration Time
	tkn.Claims["nbf"] = time.Now().UTC().Add(-leeway).Unix()         // Not Before
	tkn.Claims["iat"] = time.Now().UTC().Unix()                      // Issued At
	tkn.Claims["jti"] = tknID.String()                               // JWT ID

	signed, err := tkn.SignedString([]byte(privateKey))
	if err != nil {
		return "", errors.New("Server error: Cannot generate a token")
	}

	return signed, nil
}
