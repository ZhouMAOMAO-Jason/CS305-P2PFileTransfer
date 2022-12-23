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

snd_hash = []
rcv_hash = []
sessions = {}


class sender_rdt:
    def __init__(self, window_size):
        self.window_size = window_size
        self.window = [0] * window_size
        self.base = 1
        self.next_seq = 1
        self.buffer = []
        self.timer = 0
        self.timeout_interval = 1
        self.estimatedRTT = 0
        self.devRTT = 0
        self.alpha = 0.125
        self.beta = 0.25

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


class receiver_rdt():
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


def process_download(sock, chunkfile, outputfile):
    '''
    if DOWNLOAD is used, the peer will keep getting files until it is done
    '''
    # print('PROCESS GET SKELETON CODE CALLED.  Fill me in! I\'ve been doing! (', chunkfile, ',     ', outputfile, ')')
    global ex_output_file
    global ex_received_chunk
    global ex_downloading_chunkhash

    ex_output_file = outputfile
    # Step 1: read chunkhash to be downloaded from chunkfile
    download_hash = bytes()
    with open(chunkfile, 'r') as cf:
        index, datahash_str = cf.readline().strip().split(" ")
        ex_received_chunk[datahash_str] = bytes()
        ex_downloading_chunkhash = datahash_str

        # hex_str to bytes
        datahash = bytes.fromhex(datahash_str)
        download_hash = download_hash + datahash

    # Step2: make WHOHAS pkt
    # |2byte magic|1byte type |1byte team|
    # |2byte  header len  |2byte pkt len |
    # |      4byte  seq                  |
    # |      4byte  ack                  |
    whohas_header = struct.pack("!HBBHHII", 52305,35, 0, HEADER_LEN, HEADER_LEN+len(download_hash), 0, 0)
    whohas_pkt = whohas_header + download_hash

    # Step3: flooding whohas to all peers in peer list
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
    window_size = 20

    # Receive pkt
    pkt, from_addr = sock.recvfrom(BUF_SIZE)
    Magic, Team, Type, hlen, plen, Seq, Ack = struct.unpack("!HBBHHII", pkt[:HEADER_LEN])
    data = pkt[HEADER_LEN:]

    hash = '3b68110847941b84e8d05417a5b2609122a56314'

    if Type == 0:
        # received an WHOHAS pkt
        # see what chunk the sender has
        whohas_chunk_hash = data[:20]
        # bytes to hex_str
        chunkhash_str = bytes.hex(whohas_chunk_hash)
        ex_sending_chunkhash = chunkhash_str

        if chunkhash_str not in snd_hash:
            snd_hash.append(chunkhash_str)
            sess = {'window_size': 20, 'window': [0] * 20, 'base': 1, 'next_seq': 1, 'buffer': [], 'timer': 0,
                    'timeout_interval': 1, 'estimatedRTT': 0,
                    'devRTT': 0, 'alpha': 0.125, 'beta': 0.25}

            session_object = sender_rdt(20)
            sessions[chunkhash_str] = session_object


        print(f"whohas: {chunkhash_str}, has: {list(config.haschunks.keys())}")
        if chunkhash_str in config.haschunks:
        # send back IHAVE pkt
            ihave_header = struct.pack("!HBBHHII", 52305, 35, 1, HEADER_LEN, HEADER_LEN+len(whohas_chunk_hash), 0, 0)
            ihave_pkt = ihave_header+whohas_chunk_hash
            sock.sendto(ihave_pkt, from_addr)
    elif Type == 1:
        # received an IHAVE pkt
        # see what chunk the sender has
        get_chunk_hash = data[:20]

        # send back GET pkt
        get_header = struct.pack("!HBBHHII", 52305, 35, 2, HEADER_LEN, HEADER_LEN + len(get_chunk_hash), 0, 0)
        get_pkt = get_header + get_chunk_hash
        sock.sendto(get_pkt, from_addr)
    elif Type == 2:
        # received a GET pkt
        # chunk_data = config.haschunks[ex_sending_chunkhash][:MAX_PAYLOAD]
        #
        # # send back DATA
        # data_header = struct.pack("HBBHHII", socket.htons(52305), 35, 3, socket.htons(HEADER_LEN),
        #                           socket.htons(HEADER_LEN), socket.htonl(1), 0)
        # sock.sendto(data_header + chunk_data, from_addr)
        print(sessions[hash])
        sessions[hash].timer = time.time()
        for i in range(sessions[hash].window_size):
            left = (i) * MAX_PAYLOAD
            right = min((i + 1) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
            next_data = config.haschunks[ex_sending_chunkhash][left: right]
            # send next data
            data_header = struct.pack("!HBBHHII", 52305, 35, 3, HEADER_LEN, HEADER_LEN, 1, 0)
            sock.sendto(data_header + next_data, from_addr)
            sessions[hash].next_seq += 1
    elif Type == 3:
        # received a DATA pkt
        ex_received_chunk[ex_downloading_chunkhash] += data

        # send back ACK
        ack_pkt = struct.pack("!HBBHHII", 52305, 35, 4, HEADER_LEN, HEADER_LEN, 0, Seq)
        sock.sendto(ack_pkt, from_addr)

        # see if finished
        if len(ex_received_chunk[ex_downloading_chunkhash]) == CHUNK_DATA_SIZE:
            # finished downloading this chunkdata!
            # dump your received chunk to file in dict form using pickle
            with open(ex_output_file, "wb") as wf:
                pickle.dump(ex_received_chunk, wf)

            # add to this peer's haschunk:
            config.haschunks[ex_downloading_chunkhash] = ex_received_chunk[ex_downloading_chunkhash]

            # you need to print "GOT" when finished downloading all chunks in a DOWNLOAD file
            print(f"GOT {ex_output_file}")

            # The following things are just for illustration, you do not need to print out in your design.
            sha1 = hashlib.sha1()
            sha1.update(ex_received_chunk[ex_downloading_chunkhash])
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
        # received an ACK pkt
        cur_session = sessions[hash]
        recv_time = time.time()
        sampleRTT = recv_time - cur_session.timer
        cur_session.estimatedRTT = (1 - cur_session.alpha) * cur_session.estimatedRTT+ \
                                         cur_session.alpha * sampleRTT
        cur_session.devRTT = (1 - cur_session.beta) * cur_session.devRTT + cur_session.beta * (
            abs(sampleRTT - cur_session.estimatedRTT))
        cur_session.timeout_interval = cur_session.estimatedRTT + 4 * cur_session.devRTT

        ack_num = Ack
        print(ack_num)
        print(cur_session.base)
        cur_session.window[ack_num-cur_session.base] = 1
        if (ack_num) * MAX_PAYLOAD >= CHUNK_DATA_SIZE:
            # finished
            print(f"finished sending {ex_sending_chunkhash}")
            pass
        else:
            # left = (ack_num) * MAX_PAYLOAD
            # right = min((ack_num + 1) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
            # next_data = config.haschunks[ex_sending_chunkhash][left: right]
            # # send next data
            # data_header = struct.pack("HBBHHII", socket.htons(52305), 35, 3, socket.htons(HEADER_LEN),
            #                           socket.htons(HEADER_LEN + len(next_data)), socket.htonl(ack_num + 1), 0)
            # sock.sendto(data_header + next_data, from_addr)

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
                    cur_session.window_size[cur_session.base] += cur_session.window_size
                    cur_session.window = [0] * cur_session.window_size
                else:  # 0就先不用管这个ack了，但后面fast retransmission可能用到
                    left = copy.deepcopy(cur_session.window[x:])
                    zeros = [0] * ( cur_session.window_size- len(left) )
                    cur_session.window = left + zeros

            if (cur_session.next_seq < cur_session.base + cur_session.window_size):
                left = (cur_session.next_seq) * MAX_PAYLOAD
                right = min((cur_session.next_seq + 1) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                next_data = config.haschunks[ex_sending_chunkhash][left: right]
                # send next data
                data_header = struct.pack("!HBBHHII", 52305, 35, 3, HEADER_LEN, HEADER_LEN + len(next_data),
                                          ack_num + 1, 0)
                sock.sendto(data_header + next_data, from_addr)
                if cur_session.base == cur_session.next_seq:
                    cur_session.timer = time.time()  # start_timer
                    cur_session.next_seq += 1



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
    parser.add_argument('-p', type=str, help='<peerfile>     The list of all peers', default='nodes.map')
    parser.add_argument('-c', type=str, help='<chunkfile>    Pickle dumped dictionary {chunkhash: chunkdata}')
    parser.add_argument('-m', type=int, help='<maxconn>      Max # of concurrent sending')
    parser.add_argument('-i', type=int, help='<identity>     Which peer # am I?')
    parser.add_argument('-v', type=int, help='verbose level', default=0)
    parser.add_argument('-t', type=int, help="pre-defined timeout", default=0)
    args = parser.parse_args()

    config = bt_utils.BtConfig(args)
    peer_run(config)
