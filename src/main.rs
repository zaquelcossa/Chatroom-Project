
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

// Shared membership list:
// Username -> Client's TCP connection
type Membership = Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>;

// Maximum protocol line size
const MAX_LINE_SIZE: usize = 8192;

// Maximum message length
const MAX_MESSAGE_LENGTH: usize = 1000;

// Send a line to one client
fn send_line(writer: &Arc<Mutex<TcpStream>>, message: &str) {
    let mut stream = writer.lock().unwrap();

    if let Err(error) = stream.write_all(message.as_bytes()) {
        println!("Error sending message: {}", error);
    }
}

// Display current membership
fn print_members(members: &HashMap<String, Arc<Mutex<TcpStream>>>) {
    println!("\nCurrent chatroom members:");

    if members.is_empty() {
        println!("No users currently in the chatroom.");
    } else {
        for username in members.keys() {
            println!("- {}", username);
        }
    }

    println!();
}

// Broadcast a message to a collection of clients
fn broadcast(
    recipients: &[Arc<Mutex<TcpStream>>],
    message: &str,
) {
    for writer in recipients {
        let mut stream = writer.lock().unwrap();

        if let Err(error) = stream.write_all(message.as_bytes()) {
            println!("Error broadcasting message: {}", error);
        }
    }
}

// Remove a user and announce their departure
fn leave_room(username: &str, members: &Membership) {
    let mut members = members.lock().unwrap();

    if members.remove(username).is_some() {
        println!("{} has left the chatroom.", username);

        print_members(&members);

        // Collect remaining members
        let recipients: Vec<_> = members.values().cloned().collect();

        // Announce departure to remaining members
        let message = format!("LEFT {}\r\n", username);

        broadcast(&recipients, &message);
    }
}

// Read one complete protocol line ending in CRLF
fn read_protocol_line<R: Read>(
    reader: &mut R,
) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        match reader.read(&mut byte)? {
            0 => return Ok(None),

            _ => {
                line.push(byte[0]);

                // Check for CRLF
                if line.ends_with(b"\r\n") {
                    return Ok(Some(line));
                }

                // Close connection if 8192 bytes arrive without CRLF
                if line.len() >= MAX_LINE_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Line exceeded 8192 bytes",
                    ));
                }
            }
        }
    }
}

// Validate username according to the protocol
fn validate_username(username: &str) -> &'static str {
    // 104: Username is missing or empty
    if username.is_empty() {
        return "104";
    }

    // 102: Username is longer than 16 characters
    if username.chars().count() > 16 {
        return "102";
    }

    // 103: Username contains invalid characters
    if !username.chars().all(|c| c.is_ascii_alphanumeric()) {
        return "103";
    }

    "100"
}

// Handle one client connection
fn handle_client(stream: TcpStream, members: Membership) {
    let address = stream.peer_addr().unwrap();

    println!("Client connected: {}", address);
    println!("I have a connection!");

    // Clone TCP connection for writing
    let writer = Arc::new(Mutex::new(
        stream.try_clone().expect("Failed to clone stream")
    ));

    // Original connection is used for reading
    let mut reader = stream;

    // Username is None until successful JOIN
    let mut username: Option<String> = None;

    loop {
        // Read one complete protocol line
        let line = match read_protocol_line(&mut reader) {
            Ok(Some(line)) => line,

            Ok(None) => {
                println!("Client disconnected: {}", address);
                break;
            }

            Err(error) => {
                println!("Connection error for {}: {}", address, error);
                break;
            }
        };

        // Remove CRLF from the command
        let command_bytes = &line[..line.len() - 2];

        // Check UTF-8 validity
        let command = match std::str::from_utf8(command_bytes) {
            Ok(command) => command,

            Err(_) => {
                send_line(&writer, "500\r\n");
                continue;
            }
        };

        println!("Client {} sent: {}", address, command);

        // ==========================================
        // JOIN COMMAND
        // ==========================================

        if command == "JOIN" || command.starts_with("JOIN ") {

            // 500: Client already joined
            if username.is_some() {
                send_line(&writer, "500\r\n");
                continue;
            }

            // Extract username
            let new_username = if command == "JOIN" {
                ""
            } else {
                &command[5..]
            };

            // Validate username
            let status = validate_username(new_username);

            if status != "100" {
                send_line(&writer, &format!("{}\r\n", status));
                continue;
            }

            let mut members = members.lock().unwrap();

            // 101: Username already in use
            if members.contains_key(new_username) {
                drop(members);
                send_line(&writer, "101\r\n");
                continue;
            }

            // Add username to membership
            let new_username = new_username.to_string();

            members.insert(new_username.clone(), writer.clone());

            username = Some(new_username.clone());

            println!("{} successfully joined!", new_username);

            print_members(&members);

            // Announce JOINED to everyone, including new user
            let message = format!("JOINED {}\r\n", new_username);

            let recipients: Vec<_> = members.values().cloned().collect();

            broadcast(&recipients, &message);

            drop(members);

            // 100: Successfully joined
            send_line(&writer, "100\r\n");
        }

        // ==========================================
        // EXIT COMMAND
        // ==========================================

        else if command == "EXIT" {

            // Remove user and announce departure
            if let Some(name) = username.take() {
                leave_room(&name, &members);
            }

            // 200: Successfully exited
            send_line(&writer, "200\r\n");

            println!("Client {} exited.", address);

            // Close connection
            break;
        }

        // ==========================================
        // PUBLIC MESSAGE
        // ==========================================

        else if command == "PUBLIC" || command.starts_with("PUBLIC ") {

            // 500: Must join before sending messages
            let sender = match &username {
                Some(name) => name.clone(),
                None => {
                    send_line(&writer, "500\r\n");
                    continue;
                }
            };

            // Extract message, preserving all spaces
            let message = if command == "PUBLIC" {
                ""
            } else {
                &command[7..]
            };

            // 500: Message contains carriage return or line feed
            if message.contains('\r') || message.contains('\n') {
                send_line(&writer, "500\r\n");
                continue;
            }

            // 301: Message exceeds 1000 characters
            if message.chars().count() > MAX_MESSAGE_LENGTH {
                send_line(&writer, "301\r\n");
                continue;
            }

            // Collect everyone currently in the room
            let recipients: Vec<_> = {
                let members = members.lock().unwrap();
                members.values().cloned().collect()
            };

            // 300: Public message accepted
            send_line(&writer, "300\r\n");

            // Format message with sender's username
            let formatted_message =
                format!("PUBLIC {} {}\r\n", sender, message);

            println!("Public message from {}: {}", sender, message);

            // Deliver to everyone, including sender
            broadcast(&recipients, &formatted_message);
        }

        // ==========================================
        // PRIVATE MESSAGE
        // ==========================================

        else if command == "PRIVATE" || command.starts_with("PRIVATE ") {

            // 500: Must join before sending messages
            let sender = match &username {
                Some(name) => name.clone(),
                None => {
                    send_line(&writer, "500\r\n");
                    continue;
                }
            };

            // Extract recipient and message
            let remainder = if command == "PRIVATE" {
                ""
            } else {
                &command[8..]
            };

            // Recipient is the first field
            let (recipient, message) = match remainder.find(' ') {
                Some(position) => (
                    &remainder[..position],
                    &remainder[position + 1..],
                ),

                None => (remainder, ""),
            };

            // 403: Invalid or missing recipient
            if validate_username(recipient) != "100" {
                send_line(&writer, "403\r\n");
                continue;
            }

            // 500: Message contains carriage return or line feed
            if message.contains('\r') || message.contains('\n') {
                send_line(&writer, "500\r\n");
                continue;
            }

            // 402: Message exceeds 1000 characters
            if message.chars().count() > MAX_MESSAGE_LENGTH {
                send_line(&writer, "402\r\n");
                continue;
            }

            // Find recipient in membership
            let recipient_writer = {
                let members = members.lock().unwrap();
                members.get(recipient).cloned()
            };

            // 401: Recipient is not in the room
            let recipient_writer = match recipient_writer {
                Some(writer) => writer,

                None => {
                    send_line(&writer, "401\r\n");
                    continue;
                }
            };

            // 400: Private message accepted
            send_line(&writer, "400\r\n");

            // Format private message
            let formatted_message =
                format!("PRIVATE {} {}\r\n", sender, message);

            println!(
                "Private message from {} to {}: {}",
                sender, recipient, message
            );

            // Send only to recipient
            send_line(&recipient_writer, &formatted_message);
        }

        // ==========================================
        // OTHER COMMANDS
        // ==========================================

        else {
            // Unknown or unsupported commands
            // JOINED and LEFT cannot be sent by clients
            send_line(&writer, "500\r\n");
        }
    }

    // Remove user if connection closes unexpectedly
    if let Some(name) = username.take() {
        leave_room(&name, &members);
    }
}

// ==========================================
// MAIN SERVER
// ==========================================

fn main() {

    // Shared membership across all client threads
    let members: Membership = Arc::new(
        Mutex::new(HashMap::new())
    );

    // Listen on port 3500
    let listener = TcpListener::bind("0.0.0.0:3500")
        .expect("Failed to bind to port 3500");

    println!("Chatroom server listening on port 3500...");

    // Accept incoming connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let members = Arc::clone(&members);

                // Create separate thread for each client
                thread::spawn(move || {
                    handle_client(stream, members);
                });
            }

            Err(error) => {
                println!("Error accepting connection: {}", error);
            }
        }
    }
}

