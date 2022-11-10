package database

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DBClient struct {
	connection *sqlx.DB
	Config     DBParams
}

type DBParams struct {
	User        string
	Password    string
	Host        string
	Name        string
	Port        string
	SSLMode     string
	SSLRootCert string
	Table       string
}

func NewDBClient(config DBParams) (*DBClient, error) {
	dbClient := DBClient{
		Config: config,
	}
	err := dbClient.Connect()
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}
	return &dbClient, nil
}

func (d *DBClient) GetConnection() (connection *sqlx.DB, err error) {
	connectionStringTemplate := "host=%s user=%s password=%s port=%s sslmode=%s"

	if d.Config.SSLMode != "disable" {
		connectionStringTemplate = connectionStringTemplate + " sslrootcert=" + d.Config.SSLRootCert
	}

	connStr := fmt.Sprintf(
		connectionStringTemplate,
		d.Config.Host,
		d.Config.User,
		d.Config.Password,
		d.Config.Port,
		d.Config.SSLMode)

	//d.Config.Name is empty before creating the test database
	if d.Config.Name != "" {
		connStr = connStr + " dbname=" + d.Config.Name
	}

	if connection, err = sqlx.Connect("postgres", connStr); err != nil {
		return nil, err
	} else {
		return connection, nil
	}
}

func (d *DBClient) Connect() (err error) {
	if d.connection != nil {
		return nil
	}

	if d.connection, err = d.GetConnection(); err != nil {
		return fmt.Errorf("error connecting to %s:%s/%s as %s : %s", d.Config.Host, d.Config.Port, d.Config.Name, d.Config.User, err)
	}

	return nil
}

func (d *DBClient) runQuery(query string) (*sqlx.Rows, error) {
	if d.connection == nil {
		return nil, errors.Wrap(errors.New("cannot run query because there is no database connection"), 0)
	}
	rows, err := d.connection.Queryx(query)

	if err != nil {
		return nil, errors.Wrap(fmt.Errorf("error executing query (%s) : %w", query, err), 0)
	}

	return rows, nil
}

func (d *DBClient) closeRows(rows *sqlx.Rows) {
	if rows != nil {
		err := rows.Close()
		if err != nil {
			fmt.Println(errors.Wrap(err, 0))
		}
	}
}
