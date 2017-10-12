/* This file contains data structures and methods common to all service */

namespace cpp shared
namespace py shared
namespace java shared

// Data type for common responses and ACK
enum responseType {
	OK = 1,
	ERROR
}

struct response {
	1: responseType message
}

enum uploadResponseType {
	OK = 1,
    MISSING_BLOCKS,
    FILE_ALREADY_PRESENT,
    ERROR
}

struct uploadResponse {
	1: uploadResponseType status,
	2: list<string> hashList,
	3: list<list<string>> blockServerList
}

struct file {
	1: string filename,
	2: i32 version,
	3: list<string> hashList,
	5: responseType status
}

// Add any data structure you need here
