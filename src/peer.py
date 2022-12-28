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

PACKET_FORMAT = "!HBBHHII"
BUF_SIZE = 1400
HEADER_LEN = struct.calcsize(PACKET_FORMAT)
CHUNK_DATA_SIZE = 512 * 1024
MAX_PAYLOAD = 1024

config = None
ex_output_file = None
ex_received_chunk = dict()
# ex_sending_chunkhash = ""
# ex_downloading_chunkhash = ""
current_chunkhash_byte = ""
current_chunkhash_str = ""

# 当前peer收到的所有chunk的所有包，格式为{chunkhash: {sequence: data}}
received_chunks: Dict[str, Dict[int, bytes]] = dict() # dict[str, dict[int, bytes]]

snd_hash = []
rcv_hash = []
sessions = {}
timeout = 20


class sender_rdt:
    def __init__(self, window_size):
        self.window_size = 20
        self.window = [0] * window_size
        self.ssthresh = 64
        self.base = 1
        self.next_seq = 1
        self.buffer = []
        self.timer = 0
        self.timeout_interval = 1
        self.estimatedRTT = 0
        self.devRTT = 0
        self.alpha = 0.125
        self.beta = 0.25
        self.dup_ack = 0
        self.congestion = False
        self.OK = False

    def congestion_control(self, is_dup, timeout):
        # 要先判断收到的包是不是duplicate的，以及是否传输超时
        if is_dup:
            self.dup_ack += 1

        if not self.congestion:
            #  超时重传或收到三个多余的ack包
            if self.dup_ack >= 3 or timeout:
                self.ssthresh = max(self.window_size // 2, 2)
                self.window_size = 1
                self.window = [0] * self.window_size
                # Todo:重传
            # 正常情况
            elif self.window_size < self.ssthresh:
                self.window_size += 1
                self.window = [0] * self.window_size
            else:
                self.congestion = True

        else:
            if self.dup_ack >= 3 or timeout:
                self.ssthresh = max(self.window_size // 2, 2)
                self.window_size = 1
                self.window = [0] * self.window_size
                self.congestion = False
                # Todo:重传
            self.window_size = int(self.window_size + (1 / self.window_size))
            self.window = [0] * self.window_size

    def rdt_send(self):
        if (self.next_seq < self.base + self.window_size):
            data = self.buffer[self.next_seq]
            sndpkt = self.make_pkt(self.next_seq, data)
            # send(sndpkt)
            if self.base == self.next_seq:
                self.timer = time.time()  # start_timer
                self.next_seq += 1
            return True
        else:
            # refuse_data?
            pass

    def timeout(self):
        self.timer = time.time()
        for i in range(self.base, self.next_seq):
            data = self.buffer[i]
            sndpkt = self.make_pkt(i, data)
            # self.window = [0] * self.window_size #全部重发，ack归0 (这个需要吗，可能ack最后一个丢了，其实都收到了？
            # send(sndpkt)

    def rev_ack(self, rcvpkt):
        if (self.is_corrupt(rcvpkt)):
            pass
        else:
            ack = self.get_ack_num(rcvpkt)
            if ack < self.base + self.window_size:
                self.window[ack - self.base] = 1

            if self.base == self.next_seq:
                # stop timer 相当于结束了吧,这里看看回放？
                pass
            else:
                pass

            if ack == self.base:
                recv_time = time.time()
                sampleRTT = recv_time - self.timer
                self.estimatedRTT = (1 - self.alpha) * self.estimatedRTT + self.alpha * sampleRTT
                self.devRTT = (1 - self.beta) * self.devRTT + self.beta * (abs(sampleRTT - self.estimatedRTT))
                self.timeout_interval = self.estimatedRTT + 4 * self.devRTT
                x = 0
                for i in range(len(self.window)):
                    if self.window[i] == 1:
                        x += 1
                    else:
                        break
                if x == 0:
                    pass
                elif x == self.window_size:
                    self.base += self.window_size
                    self.window = [0] * self.window_size
                else:  # 0就先不用管这个ack了，但后面fast retransmission可能用到
                    right = copy.deepcopy(self.window[x:])
                    zeros = [0] * len(right)
                    self.window = zeros + right

    def action(self):
        while True:
            if self.next_seq == len(self.buffer):
                return True  # true代表告诉sender发完了
            time_cost = time.time() - self.timer

            if (time_cost > self.timeout_interval):
                self.timeout()
            # if 可发
            self.rdt_send()
            # if (rcv_ack):
            #     self.rev_ack()

        # return false

    def make_pkt(self, next_seq, data):
        # create packet with checksum
        return None

    # def cal_checksum(self):
    #     pass
    #

    def is_corrupt(self, rcvpkt):
        # 判断这个是不是合法的pkt
        return False

    def get_ack_num(self, rcvpkt):
        # get rcvpkt
        return 0


class receiver_rdt:
    def __init__(self, ):
        self.buffer = []
        self.timer = 0
        self.expected_seq = 0
        self.ack = 0

    def action(self):
        pkt = self.make_pkt(self.expected_seq, self.ack)
        # send(pkt)
        while True:
            if (True):  # rdt_rcv(pkt)
                # extract

                pkt = self.make_pkt(self.expected_seq, self.ack)
                # send(pkt)

                self.expected_seq += 1

    def make_pkt(self, expected_seq, ack):
        return None

session_object = sender_rdt(20)

if timeout != 0:
    session_object.timeout_interval = timeout
else:
    session_object.timeout_interval = 1

sessions['3b68110847941b84e8d05417a5b2609122a56314'] = session_object


def process_download(sock: simsocket.SimSocket, chunkfile: str, outputfile: str):
    '''
    if DOWNLOAD is used, the peer will keep getting files until it is done
    '''
    global ex_output_file
    # global ex_received_chunk
    global received_chunks
    # global ex_downloading_chunkhash

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
        # ex_downloading_chunkhash = datahash_str

        datahash = bytes.fromhex(datahash_str)

        whohas_header = struct.pack(
            PACKET_FORMAT, 52305, 35, 0, HEADER_LEN, HEADER_LEN+len(datahash), 0, 0)
        whohas_pkt = whohas_header + datahash + datahash

        # 广播给所有的peer
        peer_list = config.peers
        for p in peer_list:
            id, host, port = p
            if int(id) != config.identity:
                sock.sendto(whohas_pkt, (host, int(port)))


def process_inbound_udp(sock: simsocket.SimSocket):
    global config
    global ex_output_file
    global ex_received_chunk
    # global ex_downloading_chunkhash
    # global ex_sending_chunkhash
    global received_chunks
    global current_chunkhash_byte
    global current_chunkhash_str
    # Receive pkt
    pkt, from_addr = sock.recvfrom(BUF_SIZE)
    print(type(from_addr))
    print(from_addr)
    Magic, Team, Type, hlen, plen, Seq, Ack = struct.unpack(PACKET_FORMAT, pkt[:HEADER_LEN])
    current_chunkhash_byte = pkt[HEADER_LEN: HEADER_LEN+20]
    data = pkt[HEADER_LEN+20:]
    current_chunkhash_str = bytes.hex(current_chunkhash_byte)

    print('current_chunkhash_str: ', current_chunkhash_str)


    # 判断是否超时，超时的话要重传
    # hash = '3b68110847941b84e8d05417a5b2609122a56314'
    # print(sessions[hash].window)

    if Type == 0:
        # received an WHOHAS pkt
        # see what chunk the sender has
        whohas_chunk_hash = data[:20]
        # bytes to hex_str
        chunkhash_str = bytes.hex(whohas_chunk_hash)
        # ex_sending_chunkhash = chunkhash_str

        print(f"whohas: {chunkhash_str}, has: {list(config.haschunks.keys())}")
        if chunkhash_str in config.haschunks:
            # send back IHAVE pkt
            ihave_header = struct.pack(PACKET_FORMAT, 52305, 35, 1,
                HEADER_LEN, HEADER_LEN+len(whohas_chunk_hash), 0, 0)
            ihave_pkt = ihave_header + current_chunkhash_byte + whohas_chunk_hash
            sock.sendto(ihave_pkt, from_addr)
    elif Type == 1:
        # received an IHAVE pkt
        # see what chunk the sender has
        get_chunk_hash = data[:20]

        # send back GET pkt
        get_header = struct.pack(PACKET_FORMAT, 52305, 35, 2,
            HEADER_LEN, HEADER_LEN+len(get_chunk_hash), 0, 0)
        get_pkt = get_header + current_chunkhash_byte + get_chunk_hash
        sock.sendto(get_pkt, from_addr)
    elif Type == 2:
        # # received a GET pkt
        sessions[current_chunkhash_str].timer = time.time()
        for i in range(sessions[current_chunkhash_str].window_size):
            left = (i) * MAX_PAYLOAD
            right = min((i + 1) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
            next_data = config.haschunks[current_chunkhash_str][left: right]
            # send next data
            data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN, HEADER_LEN + len(next_data),
                                          i+1, 0)
            sock.sendto(data_header + current_chunkhash_byte + next_data, from_addr)
            sessions[current_chunkhash_str].next_seq += 1
            print(i)
            time.sleep(0.1)
    elif Type == 3:
        # 收到 DATA
        # ex_received_chunk[ex_downloading_chunkhash] += data

        # 如果当前序列号为Seq的包已经被收过了
        if Seq in received_chunks[current_chunkhash_str]:
            return
        
        received_chunks[current_chunkhash_str][Seq] = data

        # send back ACK
        ack_pkt = struct.pack(PACKET_FORMAT, 52305, 35, 4, 
            HEADER_LEN, HEADER_LEN, 0, Seq)
        sock.sendto(ack_pkt + current_chunkhash_byte, from_addr)

        # if len(ex_received_chunk[ex_downloading_chunkhash]) == CHUNK_DATA_SIZE:
        # 判断当前chunk下载是否结束
        # 计算收到的总长度

        total_len = 0
        all_packets = received_chunks[current_chunkhash_str]

        for seq, packet_data in all_packets.items():
            total_len += len(packet_data)
        
        print('total_len: ', total_len)
        if total_len == CHUNK_DATA_SIZE:
            # 先按照seq顺序将data拼接好
            final_data = bytes()
            sorted_packets = sorted(received_chunks[current_chunkhash_str])
            for seq in sorted_packets:
                final_data += received_chunks[current_chunkhash_str][seq]
            # 保存下载文件
            ex_received_chunk[current_chunkhash_str] = final_data
            with open(ex_output_file, "wb") as wf:
                pickle.dump(ex_received_chunk, wf)

            # 将该文件加入到 haschunks中
            # config.haschunks[ex_downloading_chunkhash] = ex_received_chunk[ex_downloading_chunkhash]
            config.haschunks[current_chunkhash_str] = final_data

            print(f"GOT {ex_output_file}")

            sha1 = hashlib.sha1()
            # sha1.update(ex_received_chunk[ex_downloading_chunkhash])
            sha1.update(final_data)
            received_chunkhash_str = sha1.hexdigest()
            print(f"Expected chunkhash: {current_chunkhash_str}")
            print(f"Received chunkhash: {received_chunkhash_str}")
            success = current_chunkhash_str == received_chunkhash_str
            print(f"Successful received: {success}")
            if success:
                print("Congrats! You have completed the example!")
            else:
                print("Example fails. Please check the example files carefully.")
    elif Type == 4:
        cur_session = sessions[current_chunkhash_str]
        recv_time = time.time()
        sampleRTT = recv_time - cur_session.timer
        cur_session.estimatedRTT = (1 - cur_session.alpha) * cur_session.estimatedRTT + \
                                   cur_session.alpha * sampleRTT
        cur_session.devRTT = (1 - cur_session.beta) * cur_session.devRTT + cur_session.beta * (
            abs(sampleRTT - cur_session.estimatedRTT))
        cur_session.timeout_interval = cur_session.estimatedRTT + 4 * cur_session.devRTT

        # received an ACK pkt
        ack_num = Ack
        print('ack', ack_num)
        cur_session.window[ack_num - cur_session.base] = 1
        print(cur_session.window)

        if (ack_num)*MAX_PAYLOAD >= CHUNK_DATA_SIZE:
            # finished
            print(f"finished sending {current_chunkhash_str}")
            cur_session.OK = True
        else:
            if ack_num == cur_session.base:
                x = 0
                for i in range(len(cur_session.window)): #看看window的情况
                    if cur_session.window[i] == 1:
                        x += 1
                    else:
                        break
                if x == 0: #一个ack都还没收到
                    pass
                elif x == cur_session.window_size: #ack满了
                    cur_session.base += cur_session.window_size
                    cur_session.window = [0] * cur_session.window_size
                else:  # 0就先不用管这个ack了，但后面fast retransmission可能用到
                    right = copy.deepcopy(cur_session.window[x:])
                    zeros = [0] * (cur_session.window_size- len(right))
                    cur_session.window = right + zeros
                    # print('right',right)
                    # print('zeros',zeros)
                    cur_session.base += x

            if (cur_session.next_seq < cur_session.base + cur_session.window_size):
                left = (cur_session.next_seq-1) * MAX_PAYLOAD
                right = min((cur_session.next_seq) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                next_data = config.haschunks[current_chunkhash_str][left: right]
                print('left', left)
                print('right', right)
                if(cur_session.next_seq) == 512:
                    print('512left',left)
                    print('512right',right)
                # send next data
                data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN, HEADER_LEN + len(next_data),
                                          ack_num + 1, 0)
                sock.sendto(data_header + current_chunkhash_byte + next_data, from_addr)
                if cur_session.base == cur_session.next_seq:
                    cur_session.timer = time.time()  # start_timer
                    cur_session.next_seq += 1
                    print('next_seq',cur_session.next_seq)


def process_user_input(sock: simsocket.SimSocket):
    command, chunkf, outf = input().split(' ')
    if command == 'DOWNLOAD':
        process_download(sock, chunkf, outf)
    else:
        pass


def peer_run(config: bt_utils.BtConfig):
    addr = (config.ip, config.port)
    sock = simsocket.SimSocket(config.identity, addr, verbose=config.verbose)
    global timeout
    timeout = config.timeout
    try:
        while True:
            ready = select.select([sock, sys.stdin], [], [], 0.01)
            read_ready = ready[0]
            if len(read_ready) > 0:
                if sock in read_ready:
                    process_inbound_udp(sock)
                if sys.stdin in read_ready:
                    process_user_input(sock)
            else:
                # No pkt nor input arrives during this period
                pass
            for curr in sessions:  # 这里将来要改
                curr = sessions['3b68110847941b84e8d05417a5b2609122a56314']  # 这里将来要改
                if curr.OK == False:
                    time_cost = time.time() - curr.timer
                    # print(curr.next_seq)
                    if (time_cost > curr.timeout_interval):
                        for seq in range(curr.base, curr.next_seq):
                            left = (seq) * MAX_PAYLOAD
                            right = min((seq + 1) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                            next_data = config.haschunks[current_chunkhash_str][left: right]
                            # send next data
                            data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN, HEADER_LEN + len(next_data),
                                                      seq, 0)
                            sock.sendto(data_header + current_chunkhash_byte + next_data, ('127.0.0.1', 48001))  ###这里将来要改
                #######
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
