//! Opening an `amqps://` connection: TLS on the socket, then SASL EXTERNAL when the service logs
//! in with it.
//!
//! The crate opens TLS itself instead of leaving it to the client, for two reasons. A broker's TLS
//! port (5671) expects the TLS handshake as the first bytes on the socket, while the client opens
//! with the AMQP TLS protocol header that a dedicated TLS port does not answer. And the client's
//! EXTERNAL sends no initial response, so a broker that answers with the empty challenge the
//! mechanism then calls for (`ActiveMQ` Artemis does) fails the login on a frame the client does
//! not handle. EXTERNAL is therefore negotiated here, with the empty initial response, and the
//! client receives a stream on which SASL is done. Every other profile stays the client's.
//!
//! All of it runs once per connection, before the first frame of the connection is sent.

use fe2o3_amqp::frames::sasl::Frame;
use fe2o3_amqp::transport::Transport;
use fe2o3_amqp::transport::protocol_header::ProtocolHeaderCodec;
use fe2o3_amqp_types::primitives::{Binary, Symbol};
use fe2o3_amqp_types::sasl::{SaslCode, SaslInit, SaslResponse};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_util::codec::{FramedRead, FramedWrite};
use url::Url;

use super::Connector;
use crate::error::{AmqpError, box_err};

/// The stream the connection runs on once TLS is open.
#[cfg(feature = "native-tls")]
pub(crate) type Stream = tokio_native_tls::TlsStream<TcpStream>;
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
pub(crate) type Stream = tokio_rustls::client::TlsStream<TcpStream>;

/// The SASL mechanism this module negotiates.
const EXTERNAL: &str = "EXTERNAL";

/// Opens TLS on `socket` through `connector`, verifying the broker by the URL's host, and logs in with
/// SASL EXTERNAL when `external` is set.
///
/// # Errors
///
/// Returns [`AmqpError::Connect`] when the URL names no TLS server name (a bracketed IPv6 address), when the handshake fails (an untrusted broker, a
/// client certificate the broker refuses), or when the broker refuses the EXTERNAL login.
pub(crate) async fn establish(
    socket: TcpStream,
    url: &Url,
    connector: &Connector,
    external: bool,
) -> Result<Stream, AmqpError> {
    // The client's own reading: the server name is the URL's domain, which the parser leaves
    // empty for a bracketed IPv6 address.
    let Some(domain) = url.domain() else {
        return Err(AmqpError::Connect(Box::from(
            "an amqps:// URL on an IPv6 address gives TLS no server name; name the host instead",
        )));
    };
    let stream = handshake(connector, domain, socket).await?;
    if !external {
        return Ok(stream);
    }
    login_external(stream, url.host_str()).await
}

#[cfg(feature = "native-tls")]
async fn handshake(
    connector: &Connector,
    domain: &str,
    socket: TcpStream,
) -> Result<Stream, AmqpError> {
    connector
        .connect(domain, socket)
        .await
        .map_err(|e| AmqpError::Connect(format!("the TLS handshake failed: {e}").into()))
}

#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
async fn handshake(
    connector: &Connector,
    domain: &str,
    socket: TcpStream,
) -> Result<Stream, AmqpError> {
    let name = ServerName::try_from(domain.to_owned()).map_err(|e| {
        AmqpError::Connect(format!("{domain:?} is not a TLS server name: {e}").into())
    })?;
    connector
        .connect(name, socket)
        .await
        .map_err(|e| AmqpError::Connect(format!("the TLS handshake failed: {e}").into()))
}

/// Runs the SASL EXTERNAL exchange on `stream` and hands the stream back where the AMQP header
/// comes next.
///
/// # Errors
///
/// Returns [`AmqpError::Connect`] when the broker does not offer EXTERNAL, refuses the login, or
/// breaks the exchange.
async fn login_external<Io>(stream: Io, hostname: Option<&str>) -> Result<Io, AmqpError>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, writer) = tokio::io::split(stream);
    let mut transport = Transport::<_, Frame>::negotiate_sasl_header(
        FramedWrite::new(writer, ProtocolHeaderCodec::new()),
        FramedRead::new(reader, ProtocolHeaderCodec::new()),
    )
    .await
    .map_err(|e| AmqpError::Connect(box_err(e)))?;

    let offered = match next(&mut transport).await? {
        Frame::Mechanisms(mechanisms) => mechanisms.sasl_server_mechanisms.0,
        other => return Err(unexpected(&other)),
    };
    if !offered
        .iter()
        .any(|mechanism| mechanism.as_str() == EXTERNAL)
    {
        let offered: Vec<&str> = offered.iter().map(Symbol::as_str).collect();
        return Err(AmqpError::Connect(
            format!(
                "the broker does not offer SASL EXTERNAL on this listener; it offers {offered:?}"
            )
            .into(),
        ));
    }
    // An empty initial response asks the broker for the identity the TLS layer established.
    send(
        &mut transport,
        Frame::Init(SaslInit {
            mechanism: Symbol::from(EXTERNAL),
            initial_response: Some(Binary::from(Vec::new())),
            hostname: hostname.map(str::to_owned),
        }),
    )
    .await?;
    loop {
        match next(&mut transport).await? {
            // The mechanism has nothing more to say, so a challenge is answered empty.
            Frame::Challenge(_) => {
                send(
                    &mut transport,
                    Frame::Response(SaslResponse {
                        response: Binary::from(Vec::new()),
                    }),
                )
                .await?;
            }
            Frame::Outcome(outcome) if matches!(outcome.code, SaslCode::Ok) => break,
            Frame::Outcome(outcome) => {
                return Err(AmqpError::Connect(
                    format!(
                        "the broker refused the SASL EXTERNAL login: {:?}",
                        outcome.code
                    )
                    .into(),
                ));
            }
            other => return Err(unexpected(&other)),
        }
    }

    let (writer, reader) = transport.into_framed_codec();
    // The broker speaks next only after the client's AMQP header, so nothing is buffered; bytes
    // here would be lost to the client, and the connection with them.
    if !reader.read_buffer().is_empty() {
        return Err(AmqpError::Connect(Box::from(
            "the broker sent data before the AMQP header after the SASL outcome",
        )));
    }
    Ok(reader.into_inner().unsplit(writer.into_inner()))
}

async fn next<Io>(transport: &mut Transport<Io, Frame>) -> Result<Frame, AmqpError>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    transport.next().await.map_or_else(
        || {
            Err(AmqpError::Connect(Box::from(
                "the broker closed the connection during the SASL EXTERNAL login",
            )))
        },
        |frame| frame.map_err(|e| AmqpError::Connect(box_err(e))),
    )
}

async fn send<Io>(transport: &mut Transport<Io, Frame>, frame: Frame) -> Result<(), AmqpError>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    transport
        .send(frame)
        .await
        .map_err(|e| AmqpError::Connect(box_err(e)))
}

fn unexpected(frame: &Frame) -> AmqpError {
    AmqpError::Connect(format!("unexpected SASL frame during the EXTERNAL login: {frame:?}").into())
}
