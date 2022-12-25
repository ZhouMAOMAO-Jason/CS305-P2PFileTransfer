import copy
import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import select
import util.simsocket as simsocket
import struct
import util.bt_utils as bt_utils
import hashlib
import argparse
import pickle
from typing import Dict

"""
This is CS305 project skeleton code.
Please refer to the example files - example/dumpreceiver.py and example/dumpsender.py - to learn how to play with this skeleton.
"""

BUF_SIZE = 1400
HEADER_LEN = struct.calcsize("!HBBHHII")
CHUNK_DATA_SIZE = 512 * 1024
MAX_PAYLOAD = 1024

config = None
ex_output_file = None
ex_received_chunk = dict()
ex_sending_chunkhash = ""
ex_downloading_chunkhash = ""

# 当前peer收到的所有chunk的所有包，格式为{chunkhash: {sequence: data}}
received_chunks: Dict[str, Dict[int, bytes]] = dict() # dict[str, dict[int, bytes]]


def process_download(sock, chunkfile, outputfile):
    '''
    if DOWNLOAD is used, the peer will keep getting files until it is done
    '''
    global ex_output_file
    # global ex_received_chunk
    global received_chunks
    global ex_downloading_chunkhash

    ex_output_file = outputfile
    # download_hash = bytes()

    content = ''

    with open(chunkfile, 'r') as cf:
        content = cf.readlines()

    # 有多个download_hash的情况

    for line in content:
        index, datahash_str = line.strip().split(" ")
        # ex_received_chunk[datahash_str] = bytes()
        received_chunks[datahash_str] = dict()
        ex_downloading_chunkhash = datahash_str

        datahash = bytes.fromhex(datahash_str)

        whohas_header = struct.pack(
            "!HBBHHII", 52305, 35, 0, HEADER_LEN, HEADER_LEN+len(datahash), 0, 0)
        whohas_pkt = whohas_header + datahash

        # 广播给所有的peer
        peer_list = config.peers
        for p in peer_list:
            id, host, port = p
            if int(id) != config.identity:
                sock.sendto(whohas_pkt, (host, int(port)))


def process_inbound_udp(sock):
    global config
    global ex_output_file
    global ex_received_chunk
    global ex_downloading_chunkhash
    global ex_sending_chunkhash
    global received_chunks
    # 收到 packet
    pkt, from_addr = sock.recvfrom(BUF_SIZE)
    # packet 头部
    Magic, Team, Type, hlen, plen, Seq, Ack = struct.unpack(
        "!HBBHHII", pkt[:HEADER_LEN])
    # packet 净荷
    data = pkt[HEADER_LEN:]
    if Type == 0:
        # 收到 WHOHAS
        # see what chunk the sender has
        whohas_chunk_hash = data[:20]
        # bytes to hex_str
        chunkhash_str = bytes.hex(whohas_chunk_hash)
        ex_sending_chunkhash = chunkhash_str

        print(f"whohas: {chunkhash_str}, has: {list(config.haschunks.keys())}")
        if chunkhash_str in config.haschunks:
            # send back IHAVE pkt
            ihave_header = struct.pack("!HBBHHII", 52305, 35, 1, 
                HEADER_LEN, HEADER_LEN+len(whohas_chunk_hash), 0, 0)
            ihave_pkt = ihave_header+whohas_chunk_hash
            sock.sendto(ihave_pkt, from_addr)
    elif Type == 1:
        # 收到 IHAVE
        # see what chunk the sender has
        get_chunk_hash = data[:20]

        # send back GET pkt
        get_header = struct.pack("!HBBHHII", 52305, 35, 2, 
            HEADER_LEN, HEADER_LEN+len(get_chunk_hash), 0, 0)
        get_pkt = get_header+get_chunk_hash
        sock.sendto(get_pkt, from_addr)
    elif Type == 2:
        # 收到 GET
        chunk_data = config.haschunks[ex_sending_chunkhash][:MAX_PAYLOAD]

        # 发送 DATA 数据包
        data_header = struct.pack("!HBBHHII", 52305, 35, 3, 
            HEADER_LEN, HEADER_LEN+len(chunk_data), 1, 0)
        sock.sendto(data_header+chunk_data, from_addr)
    elif Type == 3:
        # 收到 DATA
        # ex_received_chunk[ex_downloading_chunkhash] += data

        # 如果当前序列号为Seq的包已经被收过了
        if Seq in received_chunks[ex_downloading_chunkhash]:
            return
        
        received_chunks[ex_downloading_chunkhash][Seq] = data

        # send back ACK
        ack_pkt = struct.pack("!HBBHHII", 52305, 35,  4, 
            HEADER_LEN, HEADER_LEN, 0, Seq)
        sock.sendto(ack_pkt, from_addr)

        # if len(ex_received_chunk[ex_downloading_chunkhash]) == CHUNK_DATA_SIZE:
        # 判断当前chunk下载是否结束
        # 计算收到的总长度

        total_len = 0
        all_packets = received_chunks[ex_downloading_chunkhash]

        for seq, packet_data in all_packets.items():
            total_len += len(packet_data)
        
        if total_len == CHUNK_DATA_SIZE:
            # 先按照seq顺序将data拼接好
            final_data = bytes()
            sorted_packets = sorted(received_chunks[ex_downloading_chunkhash])
            for seq in sorted_packets:
                final_data += received_chunks[ex_downloading_chunkhash][seq]
            # 保存下载文件
            with open(ex_output_file, "wb") as wf:
                # pickle.dump(ex_received_chunk, wf)
                pickle.dump(final_data, wf)

            # 将该文件加入到 haschunks中
            # config.haschunks[ex_downloading_chunkhash] = ex_received_chunk[ex_downloading_chunkhash]
            config.haschunks[ex_downloading_chunkhash] = final_data

            print(f"GOT {ex_output_file}")

            sha1 = hashlib.sha1()
            # sha1.update(ex_received_chunk[ex_downloading_chunkhash])
            sha1.update(final_data)
            received_chunkhash_str = sha1.hexdigest()
            print(f"Expected chunkhash: {ex_downloading_chunkhash}")
            print(f"Received chunkhash: {received_chunkhash_str}")
            success = ex_downloading_chunkhash == received_chunkhash_str
            print(f"Successful received: {success}")
            if success:
                print("Congrats! You have completed the example!")
            else:
                print("Example fails. Please check the example files carefully.")
    elif Type == 4:
        # 收到 ACK
        ack_num = Ack
        if (ack_num)*MAX_PAYLOAD >= CHUNK_DATA_SIZE:
            # 发送完毕
            print(f"finished sending {ex_sending_chunkhash}")
            pass
        else:
            left = (ack_num) * MAX_PAYLOAD
            right = min((ack_num+1)*MAX_PAYLOAD, CHUNK_DATA_SIZE)
            next_data = config.haschunks[ex_sending_chunkhash][left: right]
            # 发送该 chunk 的下一个 packet
            data_header = struct.pack("!HBBHHII", 52305, 35, 3, 
                HEADER_LEN, HEADER_LEN+len(next_data), ack_num+1, 0)
            sock.sendto(data_header+next_data, from_addr)


def process_user_input(sock):
    command, chunkf, outf = input().split(' ')
    if command == 'DOWNLOAD':
        process_download(sock, chunkf, outf)
    else:
        pass


def peer_run(config):
    addr = (config.ip, config.port)
    sock = simsocket.SimSocket(config.identity, addr, verbose=config.verbose)

    try:
        while True:
            ready = select.select([sock, sys.stdin], [], [], 0.1)
            read_ready = ready[0]
            if len(read_ready) > 0:
                if sock in read_ready:
                    process_inbound_udp(sock)
                if sys.stdin in read_ready:
                    process_user_input(sock)
            else:
                # No pkt nor input arrives during this period
                pass
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()


if __name__ == '__main__':
    """
    -p: Peer list file, it will be in the form "*.map" like nodes.map.
    -c: Chunkfile, a dictionary dumped by pickle. It will be loaded automatically in bt_utils. The loaded dictionary has the form: {chunkhash: chunkdata}
    -m: The max number of peer that you can send chunk to concurrently. If more peers ask you for chunks, you should reply "DENIED"
    -i: ID, it is the index in nodes.map
    -v: verbose level for printing logs to stdout, 0 for no verbose, 1 for WARNING level, 2 for INFO, 3 for DEBUG.
    -t: pre-defined timeout. If it is not set, you should estimate timeout via RTT. If it is set, you should not change this time out.
        The timeout will be set when running test scripts. PLEASE do not change timeout if it set.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', type=str, help='<peerfile>     The list of all peers', default='nodes.map')
    parser.add_argument(
        '-c', type=str, help='<chunkfile>    Pickle dumped dictionary {chunkhash: chunkdata}')
    parser.add_argument(
        '-m', type=int, help='<maxconn>      Max # of concurrent sending')
    parser.add_argument(
        '-i', type=int, help='<identity>     Which peer # am I?')
    parser.add_argument('-v', type=int, help='verbose level', default=3)
    parser.add_argument('-t', type=int, help="pre-defined timeout", default=0)
    args = parser.parse_args()

    config = bt_utils.BtConfig(args)
    peer_run(config)
