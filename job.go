package curlyq

import (
	"github.com/gofrs/uuid"
	"github.com/vmihailenco/msgpack"
)

type Job struct {
	ID      string
	Data    []byte
	Attempt uint
}

func (j *Job) message() ([]byte, error) {
	if j.ID == "" {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}

		j.ID = id.String()
	}

	return msgpack.Marshal(&j)
}

func (j *Job) fromMessage(message []byte) error {
	return msgpack.Unmarshal(message, j)
}
