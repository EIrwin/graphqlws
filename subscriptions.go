package graphqlws

import (
	"errors"
	"sync"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	log "github.com/sirupsen/logrus"
)

// ErrorsFromGraphQLErrors convert from GraphQL errors to regular errors.
func ErrorsFromGraphQLErrors(errors []gqlerrors.FormattedError) []error {
	if len(errors) == 0 {
		return nil
	}

	out := make([]error, len(errors))
	for i := range errors {
		out[i] = errors[i]
	}
	return out
}

// SubscriptionSendDataFunc is a function that sends updated data
// for a specific subscription to the corresponding subscriber.
type SubscriptionSendDataFunc func(*DataMessagePayload)

// Subscription holds all information about a GraphQL subscription
// made by a client, including a function to send data back to the
// client when there are updates to the subscription query result.
type Subscription struct {
	ID            string
	Query         string
	Variables     map[string]interface{}
	OperationName string
	Document      *ast.Document
	Fields        []string
	Connection    Connection
	SendData      SubscriptionSendDataFunc
}

// MatchesField returns true if the subscription is for data that
// belongs to the given field.
func (s *Subscription) MatchesField(field string) bool {
	if s.Document == nil || len(s.Fields) == 0 {
		return false
	}

	// The subscription matches the field if any of the queries have
	// the same name as the field
	for _, name := range s.Fields {
		if name == field {
			return true
		}
	}
	return false
}

// ConnectionSubscriptions defines a map of all subscriptions of
// a connection by their IDs.
// type ConnectionSubscriptions map[string]*Subscription

// Subscriptions defines a map of connections to a map of
// subscription IDs to subscriptions.
// type Subscriptions map[Connection]ConnectionSubscriptions

// SubscriptionManager provides a high-level interface to managing
// and accessing the subscriptions made by GraphQL WS clients.
type SubscriptionManager interface {
	// Subscriptions returns all registered subscriptions, grouped
	// by connection.
	Subscriptions() *sync.Map

	// AddSubscription adds a new subscription to the manager.
	AddSubscription(Connection, *Subscription) []error

	// RemoveSubscription removes a subscription from the manager.
	RemoveSubscription(Connection, *Subscription)

	// RemoveSubscriptions removes all subscriptions of a client connection.
	RemoveSubscriptions(Connection)
}

/**
 * The default implementation of the SubscriptionManager interface.
 */

type subscriptionManager struct {
	subscriptions *sync.Map
	schema        *graphql.Schema
	logger        *log.Entry
}

func NewSubscriptionManagerWithLogger(schema *graphql.Schema, logger *log.Entry) SubscriptionManager {
	return newSubscriptionManager(schema, logger)
}

// NewSubscriptionManager creates a new subscription manager.
func NewSubscriptionManager(schema *graphql.Schema) SubscriptionManager {
	return newSubscriptionManager(schema, NewLogger("subscriptions"))
}

func newSubscriptionManager(schema *graphql.Schema, logger *log.Entry) SubscriptionManager {
	return &subscriptionManager{schema: schema, logger: logger, subscriptions: &sync.Map{}}
}

func (m *subscriptionManager) Subscriptions() *sync.Map {
	return m.subscriptions
}

func (m *subscriptionManager) AddSubscription(
	conn Connection,
	subscription *Subscription,
) []error {
	m.logger.WithFields(log.Fields{
		"conn":         conn.ID(),
		"subscription": subscription.ID,
	}).Info("Add subscription")

	if errors := validateSubscription(subscription); len(errors) > 0 {
		m.logger.WithField("errors", errors).Warn("Failed to add invalid subscription")
		return errors
	}

	// Parse the subscription query
	document, err := parser.Parse(parser.ParseParams{
		Source: subscription.Query,
	})
	if err != nil {
		m.logger.WithField("err", err).Warn("Failed to parse subscription query")
		return []error{err}
	}

	// Validate the query document
	validation := graphql.ValidateDocument(m.schema, document, nil)
	if !validation.IsValid {
		m.logger.WithFields(log.Fields{
			"errors": validation.Errors,
		}).Warn("Failed to validate subscription query")
		return ErrorsFromGraphQLErrors(validation.Errors)
	}

	// Remember the query document for later
	subscription.Document = document

	// Extract query names from the document (typically, there should only be one)
	subscription.Fields = subscriptionFieldNamesFromDocument(document)

	// Allocate the connection's map of subscription IDs to
	// subscriptions on demand
	connSubsInterface, _ := m.subscriptions.LoadOrStore(conn, &sync.Map{})

	// Add the subscription if it hasn't already been added
	connSubs := connSubsInterface.(*sync.Map)
	if val, ok := connSubs.Load(subscription.ID); ok || val != nil {
		m.logger.WithFields(log.Fields{
			"conn":         conn.ID(),
			"subscription": subscription.ID,
		}).Warn("Cannot register subscription twice")
		return []error{errors.New("Cannot register subscription twice")}
	}

	connSubs.Store(subscription.ID, subscription)

	return nil
}

func (m *subscriptionManager) RemoveSubscription(
	conn Connection,
	subscription *Subscription,
) {
	m.logger.WithFields(log.Fields{
		"conn":         conn.ID(),
		"subscription": subscription.ID,
	}).Info("Remove subscription")

	// Remove the subscription from its connections' subscription map
	subsInterface, ok := m.subscriptions.Load(conn)
	if !ok {
		m.logger.Errorf("Couldn't remove subscription, connection doesn't exist")
		return
	}
	subs := subsInterface.(*sync.Map)
	subs.Delete(subscription.ID)

	hasElems := false
	subs.Range(func(key, value interface{}) bool {
		hasElems = true
		return false
	})

	// Remove the connection as well if there are no subscriptions left
	if !hasElems {
		m.subscriptions.Delete(conn)
	}
}

func (m *subscriptionManager) RemoveSubscriptions(conn Connection) {
	m.logger.WithFields(log.Fields{
		"conn": conn.ID(),
	}).Info("Remove subscriptions")

	// Only remove subscriptions if we know the connection
	connSubsInterface, connSubsExist := m.subscriptions.Load(conn)
	if connSubsExist {
		connSubs := connSubsInterface.(*sync.Map)
		// Remove subscriptions one by one

		connSubs.Range(func(key, value interface{}) bool {
			m.RemoveSubscription(conn, value.(*Subscription))
			return true
		})

		// Remove the connection's subscription map altogether
		m.subscriptions.Delete(conn)
	}
}

func validateSubscription(s *Subscription) []error {
	errs := []error{}

	if s.ID == "" {
		errs = append(errs, errors.New("Subscription ID is empty"))
	}

	if s.Connection == nil {
		errs = append(errs, errors.New("Subscription is not associated with a connection"))
	}

	if s.Query == "" {
		errs = append(errs, errors.New("Subscription query is empty"))
	}

	if s.SendData == nil {
		errs = append(errs, errors.New("Subscription has no SendData function set"))
	}

	return errs
}
