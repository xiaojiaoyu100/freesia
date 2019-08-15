package freesia

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	errDupKey                = Error("dup key")
	errKeyExpirationTooShort = Error("key expiration too short")
	errMissingKeyMeta        = Error("no key meta")
	errKeyMiss               = Error("key miss")
)
