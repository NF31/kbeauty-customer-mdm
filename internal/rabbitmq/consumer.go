// Package rabbitmq cable le golden record (package mdm) a RabbitMQ : meme
// triptyque que les autres consumers de l'ecosysteme (sav-ticketing-php,
// kbeauty-notifications-service) - exchange direct verifie en declaration
// passive, file quorum avec dead-letter, un message a la fois, contrat
// JSON Schema verifie a la reception, idempotence (geree ici par
// mdm.Store.Upsert, pas par une simple contrainte unique : on compare les
// horodatages pour ignorer un message en retard).
//
// Contrairement aux autres consumers de l'ecosysteme, ce service a aussi
// besoin d'ecrire sur kbeauty.exchange (pour republier le golden record) :
// son compte RabbitMQ (mdm) a donc un profil de droits different (lecture
// ET ecriture sur l'exchange partage), documente dans le README.
package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/NF31/kbeauty-customer-mdm/internal/mdm"
)

const (
	Exchange        = "kbeauty.exchange"
	Queue           = "mdm.customer-updates"
	RoutingKeyIn    = "customer.updated"
	RoutingKeyOut   = "customer.golden_record.updated"
	RejectsExchange = "mdm.rejected"
	RejectsQueue    = "mdm.rejected"
	deliveryLimit   = 5
)

// customerUpdated est la forme attendue de l'evenement entrant (deja
// verifiee contre le contrat au moment du decodage).
type customerUpdated struct {
	SourceUserID int64  `json:"sourceUserId"`
	Name         string `json:"name"`
	Email        string `json:"email"`
	OccurredAt   string `json:"occurredAt"`
}

type goldenRecordUpdated struct {
	EventType    string `json:"eventType"`
	SourceUserID int64  `json:"sourceUserId"`
	Name         string `json:"name"`
	Email        string `json:"email"`
	UpdatedAt    string `json:"updatedAt"`
}

type Consumer struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	store     mdm.Store
	validator *mdm.ContractValidator
	connected bool
}

func NewConsumer(url string, store mdm.Store, validator *mdm.ContractValidator) (*Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()

		return nil, err
	}

	c := &Consumer{conn: conn, channel: channel, store: store, validator: validator}
	if err := c.declareTopology(); err != nil {
		_ = c.Close()

		return nil, err
	}

	c.connected = true

	return c, nil
}

func (c *Consumer) declareTopology() error {
	if err := c.channel.ExchangeDeclarePassive(Exchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}

	if err := c.channel.ExchangeDeclare(RejectsExchange, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := c.channel.QueueDeclare(RejectsQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := c.channel.QueueBind(RejectsQueue, "", RejectsExchange, false, nil); err != nil {
		return err
	}

	_, err := c.channel.QueueDeclare(Queue, true, false, false, false, amqp.Table{
		"x-queue-type":           "quorum",
		"x-delivery-limit":       int32(deliveryLimit),
		"x-dead-letter-exchange": RejectsExchange,
	})
	if err != nil {
		return err
	}

	if err := c.channel.QueueBind(Queue, RoutingKeyIn, Exchange, false, nil); err != nil {
		return err
	}

	return c.channel.Qos(1, 0, false)
}

func (c *Consumer) IsConnected() bool {
	return c.connected && !c.conn.IsClosed()
}

func (c *Consumer) Close() error {
	c.connected = false
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}

	return nil
}

// Run consomme indefiniment (bloquant) ; a lancer dans sa propre goroutine.
// Pas de reconnexion automatique ici - meme choix que le consumer PHP du
// SAV (processus long simple) : en cas de coupure, un superviseur externe
// (systemd, Railway) redemarre le process.
func (c *Consumer) Run(ctx context.Context) error {
	deliveries, err := c.channel.Consume(Queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				c.connected = false

				return nil
			}
			c.handle(d)
		}
	}
}

func (c *Consumer) handle(d amqp.Delivery) {
	var in customerUpdated
	if err := c.validator.Validate(d.Body, &in); err != nil {
		slog.Warn("rejete vers mdm.rejected", "error", err)
		_ = d.Reject(false)

		return
	}

	occurredAt, err := time.Parse(time.RFC3339, in.OccurredAt)
	if err != nil {
		slog.Warn("rejete vers mdm.rejected", "error", "occurredAt invalide")
		_ = d.Reject(false)

		return
	}

	applied, err := c.store.Upsert(context.Background(), mdm.Record{
		SourceUserID: in.SourceUserID,
		Name:         in.Name,
		Email:        in.Email,
		UpdatedAt:    occurredAt,
	})
	if err != nil {
		slog.Warn("erreur, nouvel essai", "sourceUserId", in.SourceUserID, "error", err)
		_ = d.Nack(false, true)

		return
	}

	if !applied {
		slog.Info("message en retard ou deja applique, ignore", "sourceUserId", in.SourceUserID)
		_ = d.Ack(false)

		return
	}

	if err := c.publishGoldenRecordUpdated(in, occurredAt); err != nil {
		slog.Warn("golden record mis a jour mais republication echouee, nouvel essai", "error", err)
		_ = d.Nack(false, true)

		return
	}

	slog.Info("golden record mis a jour et republie", "sourceUserId", in.SourceUserID)
	_ = d.Ack(false)
}

func (c *Consumer) publishGoldenRecordUpdated(in customerUpdated, updatedAt time.Time) error {
	payload, err := json.Marshal(goldenRecordUpdated{
		EventType:    "customer.golden_record.updated",
		SourceUserID: in.SourceUserID,
		Name:         in.Name,
		Email:        in.Email,
		UpdatedAt:    updatedAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}

	return c.channel.PublishWithContext(context.Background(), Exchange, RoutingKeyOut, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         payload,
	})
}
