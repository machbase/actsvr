package trjd

import (
	"github.com/google/uuid"
)

type WorkId string

func NewWorkId() WorkId {
	// Generate UUID v6
	id, err := uuid.NewV6()
	if err != nil {
		// Fallback to v4 if v6 fails
		return WorkId(uuid.New().String())
	}
	return WorkId(id.String())
}

func (w WorkId) String() string {
	return string(w)
}

func (r *ImportRequest) NewProgress(state WorkState) *ImportProgress {
	return &ImportProgress{
		WorkId: r.WorkId,
		Src:    r.Src,
		State:  int32(state),
	}
}
