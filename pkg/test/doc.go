// Package test provides utilities for integration testing with PostgreSQL
// using testcontainers.
//
// It automatically starts a PostgreSQL container, creates a connection pool,
// and cleans up after tests complete.
//
// For example, in order to test postgres integration tests, use the following
// boilerplate for your tests:
//
//	// Global variable which will hold the connection
//	var conn test.Conn
//
//	// Start up a container and return the connection
//	func TestMain(m *testing.M) {
//				test.Main(m, func(pool pg.PoolConn) (func(), error) {
//					conn = test.Conn{PoolConn: pool}
//					return nil, nil
//				})
//			}
//
//		     // Run a test which pings the database
//				func Test_Pool_001(t *testing.T) {
//					assert := assert.New(t)
//					conn := conn.Begin(t)
//					defer conn.Close()
//
//					// Ping the database
//					assert.NoError(conn.Ping(context.Background()))
//				}
package test
