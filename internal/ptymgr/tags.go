package ptymgr

import (
	"fmt"
	"regexp"
)

var tagPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)

func ValidateTag(tag string) error {
	if tag == "" {
		return nil
	}
	if !tagPattern.MatchString(tag) {
		return fmt.Errorf("%w: tag must match [A-Za-z0-9_.:-]{1,128}", ErrInvalidRequest)
	}
	return nil
}
