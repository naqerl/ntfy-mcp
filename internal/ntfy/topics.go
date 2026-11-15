package ntfy

import (
	"errors"
	"fmt"
	"regexp"
)

// ReservedTopics are the names ntfy serves as application routes. Publishing
// or subscribing to them is refused by this gateway before any upstream call.
// Keep this list in sync with GET /v1/config on the instance.
var ReservedTopics = []string{
	"docs",
	"static",
	"file",
	"app",
	"metrics",
	"account",
	"settings",
	"signup",
	"login",
	"v1",
}

var reservedTopicSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(ReservedTopics))
	for _, topic := range ReservedTopics {
		m[topic] = struct{}{}
	}
	return m
}()

var topicPattern = regexp.MustCompile(`^[-_A-Za-z0-9]{1,64}$`)

// ErrReservedTopic is returned for names in ReservedTopics.
var ErrReservedTopic = errors.New("reserved ntfy topic")

// ErrInvalidTopic is returned for names that are not valid ntfy topics.
var ErrInvalidTopic = errors.New("invalid ntfy topic")

// IsReservedTopic reports whether topic is an ntfy application route.
func IsReservedTopic(topic string) bool {
	_, ok := reservedTopicSet[topic]
	return ok
}

// ValidateTopic validates the ntfy topic charset/length and rejects routes
// that would address the ntfy application rather than a message topic.
func ValidateTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("%w: topic is required", ErrInvalidTopic)
	}
	if !topicPattern.MatchString(topic) {
		return fmt.Errorf("%w: use 1-64 characters from [A-Za-z0-9_-]", ErrInvalidTopic)
	}
	if IsReservedTopic(topic) {
		return fmt.Errorf("%w: %q is an ntfy application route", ErrReservedTopic, topic)
	}
	return nil
}
