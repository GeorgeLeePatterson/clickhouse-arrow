mod builder;
#[cfg(feature = "cloud")]
mod cloud;
pub(crate) mod connection;
mod internal;
mod options;
mod reader;
mod response;
mod tcp;
mod writer;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::take_record_batch;
use arrow::datatypes::SchemaRef;
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use strum::AsRefStr;
use tokio::sync::{broadcast, mpsc, oneshot};

pub use self::builder::*;
pub use self::connection::ConnectionStatus;
pub(crate) use self::internal::{Message, Operation};
pub use self::options::*;
pub use self::response::*;
pub use self::tcp::Destination;
pub(crate) use self::writer::*;
use crate::arrow::utils::batch_to_rows;
use crate::constants::*;
use crate::ddl::CreateOptions;
use crate::formats::{ClientFormat, NativeFormat};
use crate::native::block::Block;
use crate::native::protocol::{CompressionMethod, ProfileEvent};
use crate::prelude::*;
use crate::query::ParsedQuery;
use crate::{ClickhouseNativeError, Progress, Result, Row};

static CLIENT_ID: AtomicU16 = AtomicU16::new(0);

/// Internal implementation of [`Client`].
pub type NativeClient = Client<NativeFormat>;
/// Implementation of [`Client`] with arrow compatibility.
pub type ArrowClient = Client<ArrowFormat>;

#[derive(Debug, Clone, Default)]
pub struct ConnectionContext {
    pub trace: Option<TraceContext>,
    #[cfg(feature = "cloud")]
    pub cloud: Option<Arc<AtomicBool>>,
}

/// Emitted clickhouse events from the underlying connection
#[derive(Debug, Clone)]
pub struct Event {
    pub event:     ClickhouseEvent,
    pub qid:       Qid,
    pub client_id: u16,
}

/// Profile and progress events from clickhouse
#[derive(Debug, Clone, AsRefStr)]
pub enum ClickhouseEvent {
    Progress(Progress),
    Profile(Vec<ProfileEvent>),
}

/// Client handle for a Clickhouse connection, has internal reference to connection,
/// and can be freely cloned and sent across threads.
#[derive(Clone, Debug)]
pub struct Client<T: ClientFormat> {
    pub client_id: u16,
    conn:          Arc<connection::Connection<T>>,
    events:        Arc<broadcast::Sender<Event>>,
    settings:      Option<Arc<Settings>>,
}

impl<T: ClientFormat> Client<T> {
    /// Connects to a specific socket address over plaintext TCP for Clickhouse.
    #[instrument(
        level = "debug",
        name = "clickhouse.connect",
        fields(
            db.system = "clickhouse",
            db.format = T::FORMAT,
            network.transport = ?if options.use_tls { "tls" } else { "tcp" }
        ),
        skip_all
    )]
    pub async fn connect<A: Into<Destination>>(
        destination: A,
        options: ClientOptions,
        settings: Option<Arc<Settings>>,
        context: Option<ConnectionContext>,
    ) -> Result<Self> {
        let context = context.unwrap_or_default();
        let trace_ctx = context.trace.unwrap_or_default();
        let _ = trace_ctx.link(&Span::current());

        let client_id = CLIENT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Resolve the destination
        let destination: Destination = destination.into();
        let addrs = destination.resolve(options.ipv4_only).await?;

        #[cfg(feature = "cloud")]
        {
            // Ping the cloud instance if requested
            if let Some(domain) = options.domain.as_ref().filter(|_| options.cloud.wakeup) {
                let cloud_track = context.cloud.as_deref();
                Self::ping_cloud(domain, options.cloud.timeout, cloud_track).await;
            }
        }

        if let Some(addr) = addrs.first() {
            let _ = Span::current()
                .record("server.address", tracing::field::debug(&addr.ip()))
                .record("server.port", addr.port());
            debug!(server.address = %addr.ip(), server.port = addr.port(), "Initiating connection");
        }

        let (event_tx, _) = broadcast::channel(EVENTS_CAPACITY);
        let events = Arc::new(event_tx);

        let conn =
            Arc::new(connection::Connection::connect(client_id, addrs, options, trace_ctx).await?);
        debug!("created connection successfully");

        Ok(Client { client_id, conn, events, settings })
    }

    pub fn status(&self) -> ConnectionStatus { self.conn.status() }

    /// Receive progress and profile on queries as they execute.
    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> { self.events.subscribe() }

    /// Check the health of the underlying connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the health check fails.
    pub async fn health_check(&self, ping: bool) -> Result<()> {
        trace!({ ATT_CID } = self.client_id, "sending health check w/ ping={ping}");
        self.conn.check_connection(ping).await
    }

    /// Shutdown the client.
    ///
    /// # Errors
    ///
    /// Returns an error if the shutdown fails.
    pub async fn shutdown(&self) -> Result<()> {
        trace!("shutting down client");
        self.conn.shutdown().await
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from
    /// Clickhouse are read.
    #[instrument(
        level = "trace",
        name = "clickhouse.insert",
        skip_all
        fields(
            db.system = "clickhouse",
            db.operation = "insert",
            db.format = T::FORMAT,
            clickhouse.client.id = self.client_id,
            clickhouse.query.id
        ),
    )]
    pub async fn insert(
        &self,
        query: impl Into<ParsedQuery>,
        block: T::Data,
        qid: Option<Qid>,
    ) -> Result<impl Stream<Item = Result<()>> + '_> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);

        // Create metadata channel
        let (tx, rx) = oneshot::channel();

        let response = self
            .conn
            .send_request(
                Operation::Insert {
                    query,
                    data: block,
                    settings: self.settings.clone(),
                    metadata: tx,
                },
                qid,
            )
            .await?;

        let Ok(metadata) = rx.await else {
            error!("Failed receive Header block");
            return Err(ClickhouseNativeError::InternalChannelError);
        };

        let qid = metadata.qid;
        trace!({ ATT_CID } = self.client_id, { ATT_QID } = %qid, "sent query, awaiting response");
        Ok(self.insert_response(response, qid))
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from
    /// Clickhouse are read.
    #[instrument(
        name = "clickhouse.insert_many",
        fields(
            db.system = "clickhouse",
            db.operation = "insert",
            db.format = T::FORMAT,
            clickhouse.client.id = self.client_id,
            clickhouse.query.id
        ),
        skip_all
    )]
    pub async fn insert_many(
        &self,
        query: impl Into<ParsedQuery>,
        batch: Vec<T::Data>,
        qid: Option<Qid>,
    ) -> Result<impl Stream<Item = Result<()>> + '_> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);

        // Create metadata channel
        let (tx, rx) = oneshot::channel();

        let response = self
            .conn
            .send_request(
                Operation::InsertMany {
                    query,
                    data: batch,
                    settings: self.settings.clone(),
                    metadata: tx,
                },
                qid,
            )
            .await?;

        let Ok(metadata) = rx.await else {
            error!("Failed receive Header block");
            return Err(ClickhouseNativeError::InternalChannelError);
        };

        let qid = metadata.qid;
        trace!({ ATT_CID } = self.client_id, { ATT_QID } = %qid, "sent query, awaiting response");
        Ok(self.insert_response(response, qid))
    }

    /// Sends a query and receive data (format specific) in raw format.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    #[instrument(
        name = "clickhouse.query",
        skip_all
        fields(
            db.system = "clickhouse",
            db.operation = "query",
            db.format = T::FORMAT,
            clickhouse.client.id = self.client_id,
            clickhouse.query.id = %qid
        ),
     )]
    pub async fn query_raw(
        &self,
        query: String,
        qid: Qid,
    ) -> Result<impl Stream<Item = Result<T::Data>> + 'static> {
        // Create metadata channel
        let (tx, rx) = oneshot::channel();
        let response = self
            .conn
            .send_request(
                Operation::Query { query, settings: self.settings.clone(), metadata: tx },
                qid,
            )
            .await?;

        let Ok(metadata) = rx.await else {
            error!("Failed receive Header block");
            return Err(ClickhouseNativeError::InternalChannelError);
        };

        let qid = metadata.qid;
        trace!({ ATT_CID } = self.client_id, { ATT_QID } = %qid, "sent query, awaiting response");
        Ok(create_response_stream::<T>(response, Arc::clone(&self.events), qid, self.client_id))
    }

    /// Same as `query`, but discards all returns blocks. Waits until the first block returns from
    /// the server to check for errors. Waiting for the first response block or EOS also
    /// prevents the server from aborting the query potentially due to client disconnection.
    #[instrument(skip_all, fields(clickhouse.client.id = self.client_id))]
    pub async fn execute(&self, query: impl Into<ParsedQuery>, qid: Option<Qid>) -> Result<()> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);
        let stream = self.query_raw(query, qid).await?;
        tokio::pin!(stream);
        while let Some(next) = stream.next().await {
            drop(next?);
        }
        Ok(())
    }

    /// Same as `execute`, but doesn't wait for a server response. The query could get aborted if
    /// the connection is closed quickly.
    #[instrument(skip_all, fields(clickhouse.client.id = self.client_id))]
    pub async fn execute_now(&self, query: impl Into<ParsedQuery>, qid: Option<Qid>) -> Result<()> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);
        drop(self.query_raw(query, qid).await?);
        Ok(())
    }

    // TODO: Support other DB engines
    /// Issue a create DDL statement for a database
    #[instrument(
        name = "clickhouse.create_database",
        skip_all
        fields(db.system = "clickhouse", db.operation = "create.database")
    )]
    pub async fn create_database(&self, database: Option<&str>, qid: Option<Qid>) -> Result<()> {
        let database = database.unwrap_or(self.conn.database());
        let database = database.to_lowercase();
        if &database == "default" {
            warn!("Exiting, cannot create `default` database");
            return Ok(());
        }

        let stmt = create_db_statement(&database)?;
        self.execute(stmt, qid).await?;
        Ok(())
    }

    /// Issue a drop DDL statement for a database
    #[instrument(
        name = "clickhouse.drop_database",
        skip_all
        fields(db.system = "clickhouse", db.operation = "drop.database")
    )]
    pub async fn drop_database(&self, database: &str, sync: bool, qid: Option<Qid>) -> Result<()> {
        let database = database.to_lowercase();
        if &database == "default" {
            warn!("Exiting, cannot drop `default` database");
            return Ok(());
        }

        // TODO: Should this check remain? Or should the query writing be modified in the case
        // of issuing DDL statements while connected to a non-default database
        let current_database = self.conn.database();
        if current_database != "default"
            && !current_database.is_empty()
            && current_database != database
        {
            error!("Cannot drop database {database} while connected to {current_database}");
            return Err(ClickhouseNativeError::InsufficientDDLScope(current_database.into()));
        }

        let stmt = drop_db_statement(&database, sync)?;
        self.execute(stmt, qid).await?;
        Ok(())
    }
}

impl<T: ClientFormat> Client<T> {
    #[cfg(feature = "cloud")]
    #[instrument(level = "trace", name = "clickhouse.cloud.ping")]
    async fn ping_cloud(domain: &str, timeout: Option<u64>, track: Option<&AtomicBool>) {
        debug!("pinging cloud instance");
        if !domain.is_empty() {
            debug!(domain, "cloud endpoint found");
            // Create receiver channel to cancel ping if dropped
            let (_tx, rx) = oneshot::channel::<()>();
            cloud::ping_cloud(domain.to_string(), timeout, track, rx).await;
        }
    }

    fn insert_response(
        &self,
        rx: mpsc::Receiver<Response<T::Data>>,
        qid: Qid,
    ) -> ClickhouseResponse<()> {
        ClickhouseResponse::<()>::from_stream(handle_insert_response::<T>(
            rx,
            Arc::clone(&self.events),
            qid,
            self.client_id,
        ))
    }
}

impl Client<NativeFormat> {
    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks
    /// Clickhouse are read.
    #[instrument(
        name = "clickhouse.insert_rows",
        fields(
            db.system = "clickhouse",
            db.operation = "insert",
            db.format = NativeFormat::FORMAT,
            clickhouse.client.id = self.client_id,
            clickhouse.query.id
        ),
        skip_all
    )]
    pub async fn insert_rows<T: Row + Send + 'static>(
        &self,
        query: impl Into<ParsedQuery>,
        blocks: impl Iterator<Item = T> + Send + Sync + 'static,
        qid: Option<Qid>,
    ) -> Result<ClickhouseResponse<()>> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);

        // Create metadata channel
        let (tx, rx) = oneshot::channel();
        let get_data =
            Box::new(move |h: Vec<(String, crate::Type)>| Block::from_rows(blocks.collect(), h));

        let response = self
            .conn
            .send_request(
                Operation::InsertRows {
                    query,
                    get_data,
                    settings: self.settings.clone(),
                    metadata: tx,
                },
                qid,
            )
            .await?;

        let metadata = rx.await.map_err(|_| ClickhouseNativeError::InternalChannelError);

        let Ok(qid) = metadata.map(|m| m.qid) else {
            return Err(ClickhouseNativeError::InternalChannelError);
        };

        let cid = self.client_id;
        trace!({ ATT_CID } = cid, { ATT_QID } = %qid, "sent query, awaiting response");

        let stream = handle_insert_response::<NativeFormat>(
            response,
            Arc::clone(&self.events),
            qid,
            self.client_id,
        );
        Ok(ClickhouseResponse::<()>::new(Box::pin(stream)))
    }

    /// Runs a query against Clickhouse, returning a stream of deserialized rows.
    /// Note that no rows are returned until Clickhouse sends a full block (but it usually sends
    /// more than one block).
    #[instrument(skip_all, fields(db.system = "clickhouse", db.operation = "query"))]
    pub async fn query<T: Row + Send + 'static>(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<Qid>,
    ) -> Result<ClickhouseResponse<T>> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);
        let raw = self.query_raw(query, qid).await?;
        Ok(ClickhouseResponse::new(Box::pin(raw.flat_map(|block| {
            match block {
                Ok(mut block) => stream::iter(
                    block
                        .take_iter_rows()
                        .filter(|x| !x.is_empty())
                        .map(T::deserialize_row)
                        .map(|maybe| maybe.inspect_err(|error| error!(?error, "deserializing row")))
                        .collect::<Vec<_>>(),
                ),
                Err(e) => stream::iter(vec![Err(e)]),
            }
        }))))
    }

    /// Same as `query`, but returns the first row and discards the rest.
    #[instrument(skip_all, fields(db.system = "clickhouse", db.operation = "query"))]
    pub async fn query_one<T: Row + Send + 'static>(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<Qid>,
    ) -> Result<Option<T>> {
        let mut stream = self.query::<T>(query, qid).await?;
        stream.next().await.transpose()
    }

    /// Issue a create DDL statement for a table
    #[instrument(
        name = "clickhouse.create_table",
        skip_all
        fields(
            db.system = "clickhouse",
            db.format = ArrowFormat::FORMAT,
            db.operation = "create.table"
        )
    )]
    pub async fn create_table<T: Row>(
        &self,
        database: Option<&str>,
        table: &str,
        options: &CreateOptions,
        qid: Option<Qid>,
    ) -> Result<()> {
        let database = database.unwrap_or(self.conn.database());
        let stmt = create_table_statement_from_native::<T>(Some(database), table, options)?;
        self.execute(stmt, qid).await?;
        Ok(())
    }
}

impl Client<ArrowFormat> {
    // TODO: As a convenience, add a method to go from T: Row to RecordBatch for inserts

    /// Runs a query against Clickhouse, returning a stream of [`RecordBatch`].
    #[instrument(
        skip_all,
        fields(db.system = "clickhouse", db.operation = "query", clickhouse.query.id)
    )]
    pub async fn query(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<Qid>,
    ) -> Result<ClickhouseResponse<RecordBatch>> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);
        Ok(ClickhouseResponse::new(Box::pin(self.query_raw(query, qid).await?)))
    }

    /// Same as `query`, but returns rows transposed to columns and wrapped in internal types
    #[instrument(
        name = "clickhouse.query_rows",
        fields(
            db.system = "clickhouse",
            db.operation = "query",
            db.format = ArrowFormat::FORMAT,
            clickhouse.client.id = self.client_id,
            clickhouse.query.id
        ),
        skip_all
    )]
    pub async fn query_rows(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<Qid>,
    ) -> Result<ClickhouseResponse<Vec<Value>>> {
        let (query, qid) = record_query(qid, query.into(), self.client_id);

        // Create metadata channel
        let (tx, rx) = oneshot::channel();
        let response = self
            .conn
            .send_request(
                Operation::Query { query, settings: self.settings.clone(), metadata: tx },
                qid,
            )
            .await?;

        let Ok(metadata) = rx.await else {
            error!("Failed receive Header block");
            return Err(ClickhouseNativeError::InternalChannelError);
        };

        let qid = metadata.qid;
        let header = metadata.header;
        trace!({ ATT_CID } = self.client_id, { ATT_QID } = %qid, "sent query, awaiting response");
        let response = create_response_stream::<ArrowFormat>(
            response,
            Arc::clone(&self.events),
            qid,
            self.client_id,
        )
        .map(move |batch| (header.clone(), batch))
        .map(|(header, batch)| {
            let batch = batch?;
            let batch_iter = batch_to_rows(&batch, header.as_deref())?;
            Ok::<_, ClickhouseNativeError>(stream::iter(batch_iter))
        })
        .try_flatten();

        Ok(ClickhouseResponse::from_stream(response))
    }

    /// Same as `query`, but returns a column of data
    #[instrument(
        name = "clickhouse.query_column",
        skip_all,
        fields(db.system = "clickhouse", db.operation = "query")
    )]
    pub async fn query_column(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<Qid>,
    ) -> Result<Option<ArrayRef>> {
        let mut stream = self.query(query, qid).await?;
        let Some(batch) = stream.next().await.transpose()? else {
            return Ok(None);
        };

        if batch.num_rows() == 0 { Ok(None) } else { Ok(Some(Arc::clone(batch.column(0)))) }
    }

    /// Same as `query`, but returns the first row and discards the rest.
    #[instrument(
        name = "clickhouse.query_one",
        skip_all
        fields(db.system = "clickhouse", db.operation = "query")
    )]
    pub async fn query_one(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<Qid>,
    ) -> Result<Option<RecordBatch>> {
        let stream = self.query(query, qid).await?;
        tokio::pin!(stream);

        let Some(batch) = stream.next().await.transpose()? else {
            return Ok(None);
        };

        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(take_record_batch(&batch, &arrow::array::UInt32Array::from(vec![0]))?))
        }
    }

    /// Fetch schemas (databases)
    ///
    /// # Returns
    ///
    /// A `Vec` of database names
    #[instrument(
        name = "clickhouse.fetch_schemas",
        skip_all
        fields(db.system = "clickhouse", db.operation = "query")
    )]
    pub async fn fetch_schemas(&self, qid: Option<Qid>) -> Result<Vec<String>> {
        crate::arrow::schema::fetch_databases(self, qid).await
    }

    /// Fetch all tables
    ///
    /// # Returns
    ///
    /// A `HashMap` of database names to table names
    #[instrument(
        name = "clickhouse.fetch_all_tables",
        skip_all
        fields(db.system = "clickhouse", db.operation = "query")
    )]
    pub async fn fetch_all_tables(&self, qid: Option<Qid>) -> Result<HashMap<String, Vec<String>>> {
        crate::arrow::schema::fetch_all_tables(self, qid).await
    }

    /// Fetch tables
    ///
    /// # Returns
    ///
    /// A `Vec` of table names
    #[instrument(
        name = "clickhouse.fetch_tables",
        skip_all
        fields(db.system = "clickhouse", db.operation = "query")
    )]
    pub async fn fetch_tables(
        &self,
        database: Option<&str>,
        qid: Option<Qid>,
    ) -> Result<Vec<String>> {
        let database = database.unwrap_or(self.conn.database());
        crate::arrow::schema::fetch_tables(self, database, qid).await
    }

    /// Fetch schemas providing a list of tables to filter on. An empty list will fetch all
    /// schemas.
    ///
    /// # Returns
    ///
    /// A `HashMap` mapping table names to their corresponding schema.
    #[instrument(
        name = "clickhouse.fetch_schema",
        skip_all
        fields(db.system = "clickhouse", db.operation = "query")
    )]
    pub async fn fetch_schema(
        &self,
        database: Option<&str>,
        tables: &[&str],
        qid: Option<Qid>,
    ) -> Result<HashMap<String, SchemaRef>> {
        let database = database.unwrap_or(self.conn.database());
        let options = self.conn.metadata().arrow_options;
        crate::arrow::schema::fetch_schema(self, database, tables, qid, options).await
    }

    /// Issue a create DDL statement for a table
    #[instrument(
        name = "clickhouse.create_table",
        skip_all
        fields(
            db.system = "clickhouse",
            db.operation = "create.table",
            db.format = ArrowFormat::FORMAT,
        )
    )]
    pub async fn create_table(
        &self,
        database: Option<&str>,
        table: &str,
        schema: &SchemaRef,
        options: &CreateOptions,
        qid: Option<Qid>,
    ) -> Result<()> {
        let database = database.unwrap_or(self.conn.database());
        let arrow_options = self.conn.metadata().arrow_options;
        let stmt = create_table_statement_from_arrow(
            Some(database),
            table,
            schema,
            options,
            Some(arrow_options),
        )?;
        self.execute(stmt, qid).await?;
        Ok(())
    }
}

impl<T: ClientFormat> Drop for Client<T> {
    fn drop(&mut self) {
        trace!({ ATT_CID } = self.client_id, "Client dropped");
    }
}

/// Simple helper to log query id and client id
fn record_query(qid: Option<Qid>, query: ParsedQuery, cid: u16) -> (String, Qid) {
    let qid = qid.unwrap_or_default();
    let _ = Span::current().record(ATT_QID, tracing::field::display(qid));
    let query = query.0;
    trace!(query, { ATT_CID } = cid, "Querying clickhouse");
    (query, qid)
}
