extern crate clap;
extern crate env_logger;
extern crate kafka;
extern crate pnet;
extern crate chrono;

use clap::App;

use kafka::client::{KafkaClient};
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::error::Error as KafkaError;

use pnet::datalink::{self, NetworkInterface};

use pnet::packet::Packet;
use pnet::packet::ipv4::Ipv4Packet;

use chrono::{DateTime, UTC};

use std::time::Duration;

fn main() {
    // CLI part
    // TODO: Refacto put in seperated file
    // TODO: Improve the CLI
    let matches = App::new("packet_tracer")
        .version("1.0")
        .author("Mickaël Fortunato <morsi.morsicus@gmail.com>")
        .about("Dump packets and send them in a kafka")
        .args_from_usage(
            "-i, --interface=[interface]    'Set an interface to listen on'
            -b, --broker=[broker]           'Set a kafka broker'
            -t, --topic=[topic]             'Set a kafka topic'
            -k, --key=[key]                 'Set a kafka key'"
        )
        .get_matches();


    let iface_name = matches.value_of("interface").unwrap();
    let broker = matches.value_of("broker").unwrap();
    let topic = matches.value_of("topic").unwrap();
    let key = matches.value_of("key").unwrap();

    // Network part
    use pnet::datalink::Channel::Ethernet;
    let interface_names_match = |iface: &NetworkInterface| iface.name == iface_name;

    // Find the network interface with the provided name
    let interfaces = datalink::interfaces();
    let interface = interfaces.into_iter().filter(interface_names_match).next().unwrap();

    // Create a channel to receive on
    let (_, mut rx) = match datalink::channel(&interface, Default::default()) {
        Ok(Ethernet(tx, rx)) => (tx, rx),
        Ok(_) => panic!("packetdump: unhandled channel type: {}"),
        Err(e) => panic!("packetdump: unable to create channel: {}", e),
    };

    let mut iter = rx.iter();
    loop {
        let packet = match iter.next() {
            Ok(packet) => packet,
            Err(e) => panic!("packetdump: unable to receive packet: {}", e),
        };

        // Deal with Ipv4 Packet
        // TODO: Don't unwrap
        // TODO: Deal with Ipv6 too
        let header = Ipv4Packet::new(packet.payload()).unwrap();

        let mac_src = packet.get_source();
        let mac_dst = packet.get_destination();
        let ip_src = header.get_source();
        let ip_dest = header.get_destination();
        let proto_used = header.get_next_level_protocol();
        let now_utc: DateTime<UTC> = UTC::now();

        // Format a string to send in kafka, in a file ...
        // TODO: Maybe a json should be more interesting (serde ?)
        let data = format!("[{}]: {} {}({}) -> {}({}) iface: {}",
                           now_utc,
                           proto_used,
                           ip_src,
                           mac_src,
                           ip_dest,
                           mac_dst,
                           iface_name);
        println!("{}", data);

        // Kafka part
        // TODO: unwrap is not acceptable here
        deal_with_kafka(data.as_bytes(), broker, topic, key).unwrap();
    }
}

// TODO: Split in other file
fn deal_with_kafka<'a, 'b>(data: &'a [u8], broker: &'b str, topic: &'b str, key: &'b str)
                           -> Result<(), KafkaError> {
    let mut client = KafkaClient::new(vec![broker.to_owned()] );
    client.set_client_id(key.into());
    client.load_metadata_all().unwrap();

    let mut producer = try!(Producer::from_client(client)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create());

    try!(producer.send(&Record {
        topic: topic,
        partition: -1,
        key: key,
        value: data }));

    Ok(())
}
