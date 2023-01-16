import copy
import sys
import os
import time
import matplotlib.pyplot as plt

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import select
import util.simsocket as simsocket
import struct
import util.bt_utils as bt_utils
import hashlib
import argparse
import pickle
from typing import Dict, Tuple

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

# 记录每个chunk最终输出的文件名称
download_filenames: Dict[str, str] = dict()

# 记录每个chunk最终的数据
final_received_chunks: Dict[str, bytes] = dict()

# 当前peer收到的所有chunk的所有包，格式为{chunkhash: {sequence: data}}
received_chunks_data: Dict[str, Dict[int, bytes]] = dict()

# 当前peer收到的所有chunk的累计acknum
received_chunks_acks: Dict[str, int] = dict()

# 当前哪些文件已经收完了
chunks_ok: Dict[str, bool] = dict()

# 是否收到冗余的ACK {chunkhash: {sequence: number}}
duplicate_ACK: Dict[str, Dict[int, int]] = dict()

# 记录窗口大小变化
win_time: Dict[float, int] = dict()

# 当前传输的包的最后一次传输时间
recv_time: Dict[str, float] = dict()

# 接收方发送whohas的时间
send_whohas_times: Dict[str, Tuple[float, bool]] = dict()

snd_hash = []
rcv_hash = []
timeout = 20


class SenderSession:
    def __init__(self, window_size: int):
        self.window_size = 20
        self.window = [0] * (window_size + 1)
        self.time_record = {}
        self.ssthresh = 64
        self.base = 1
        self.next_seq = 1
        self.buffer = []
        self.timeout_interval = 2.5
        self.estimatedRTT = 1.5
        self.devRTT = 0
        self.alpha = 0.125
        self.beta = 0.25
        self.congestion = False
        self.OK = False
        self.float_size = 20.0

    def congestion_control(self, re_transmit):
        # 要先判断收到的包是不是duplicate的，以及是否传输超时
        if not self.congestion:
            #  超时重传或收到三个多余的ack包
            if re_transmit:
                self.ssthresh = max(self.window_size // 2, 2)
                self.window_size = 1
                self.float_size = 1.0
                self.window = [0, self.window[1]]
            # 正常情况
            elif self.window_size < self.ssthresh:
                self.window_size += 1
                self.float_size += 1
                self.window = self.window + [0]
            else:
                self.congestion = True

        else:
            if re_transmit:
                self.ssthresh = max(self.window_size // 2, 2)
                self.window_size = 1
                self.float_size = 1.0
                self.window = [0, self.window[1]]
                self.congestion = False
            elif self.window_size < self.ssthresh:
                self.window_size += 1
                self.float_size += 1
                self.window = self.window[: self.window_size] + [0]
            else:
                self.float_size += 1 / self.window_size
                if self.float_size > self.window_size + 1:
                    self.window_size += 1
                    self.window = self.window[: self.window_size] + [0]

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
            self.congestion_control(False, True)
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
                    self.window = [0] * (self.window_size + 1)
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


class ReceiverSession:
    def __init__(self):
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


# {(chunkhash, ip, port): sender_session}
sender_sessions: Dict[Tuple[str, str, int], SenderSession] = {}


def win_vis():
    global win_time
    x = []
    y = []
    for t in win_time:
        x.append(t)
        y.append(win_time[t])
    plt.plot(x, y)
    plt.title("Window size over time")
    plt.savefig("win_vis.png")


def process_download(sock: simsocket.SimSocket, chunkfile: str, outputfile: str):
    '''
    if DOWNLOAD is used, the peer will keep getting files until it is done
    '''
    global download_filenames
    global chunks_ok
    global send_whohas_times

    content = ''

    with open(chunkfile, 'r') as cf:
        content = cf.readlines()

    # 有多个download_hash的情况
    # sock.add_log("self own: {}".format(config.haschunks.keys()))
    for line in content:

        index, datahash_str = line.strip().split(" ")
        # sock.add_log("datahash_str from split line: {}".format(datahash_str))
        download_filenames[datahash_str] = outputfile

        if datahash_str in config.haschunks:
            # sock.add_log("existed chunk no need request : {}".format(datahash_str))
            final_received_chunks[datahash_str] = config.haschunks[datahash_str]
            chunks_ok[datahash_str] = True
            continue

        chunks_ok[datahash_str] = False
        datahash = bytes.fromhex(datahash_str)
        whohas_header = struct.pack(
            PACKET_FORMAT, 52305, 35, 0, HEADER_LEN, HEADER_LEN + len(datahash), 0, 0)
        whohas_pkt = whohas_header + datahash + datahash

        # 广播给所有的peer
        peer_list = config.peers
        for p in peer_list:
            id, host, port = p
            if int(id) != config.identity:
                if datahash_str not in send_whohas_times:
                    send_whohas_times[datahash_str] = (time.time(), False)
                sock.sendto(whohas_pkt, (host, int(port)))
                # sock.add_log("whohas_chunkhash_str : {}".format(datahash_str))


def process_inbound_udp(sock: simsocket.SimSocket):
    global config
    global final_received_chunks
    global received_chunks_data
    global received_chunks_acks
    global duplicate_ACK
    global recv_time
    global send_whohas_times
    global chunks_ok

    # 收到包
    pkt, from_addr = sock.recvfrom(BUF_SIZE)
    ip, port = from_addr

    Magic, Team, Type, hlen, plen, Seq, Ack = struct.unpack(PACKET_FORMAT, pkt[:HEADER_LEN])
    # 当前包属于哪一个chunk
    current_chunkhash_byte = pkt[HEADER_LEN: HEADER_LEN + 20]
    data = pkt[HEADER_LEN + 20:]
    current_chunkhash_str = bytes.hex(current_chunkhash_byte)

    # print('current_chunkhash_str: ', current_chunkhash_str)

    if Type == 0:
        # 收到WHOHAS
        whohas_chunk_hash = data[:20]
        # hex转str
        chunkhash_str = bytes.hex(whohas_chunk_hash)

        # print(f"whohas: {chunkhash_str}, has: {list(config.haschunks.keys())}")
        if chunkhash_str in config.haschunks:
            # 发送IHAVE
            ihave_header = struct.pack(PACKET_FORMAT, 52305, 35, 1,
                                       HEADER_LEN, HEADER_LEN + len(whohas_chunk_hash), 0, 0)
            ihave_pkt = ihave_header + current_chunkhash_byte + whohas_chunk_hash
            sock.sendto(ihave_pkt, from_addr)
    elif Type == 1:
        # 收到IHAVE
        get_chunk_hash = data[:20]
        # 如果之前已经有chunk拥有者发过IHAVE，就不做任何动作
        # if current_chunkhash_str in received_chunks_data:
        #     return

        if current_chunkhash_str in received_chunks_acks and received_chunks_acks[current_chunkhash_str] > 0:
            return

        # 如果还没有别的chunk拥有者到来，就初始化当前chunkhash
        received_chunks_data[current_chunkhash_str] = dict()
        received_chunks_acks[current_chunkhash_str] = 1
        recv_time[current_chunkhash_str] = time.time()

        send_whohas_times[current_chunkhash_str] = (time.time(), True)

        # 发送GET
        get_header = struct.pack(PACKET_FORMAT, 52305, 35, 2,
                                 HEADER_LEN, HEADER_LEN + len(get_chunk_hash), 0, 0)
        get_pkt = get_header + current_chunkhash_byte + get_chunk_hash
        sock.sendto(get_pkt, from_addr)
    elif Type == 2:
        # 收到GET
        # 创建SenderSession实例，加入到sessions中
        new_sender_session = SenderSession(20)
        duplicate_ACK[current_chunkhash_str] = dict()

        if timeout != 0:
            new_sender_session.timeout_interval = timeout

        sender_sessions[(current_chunkhash_str, ip, port)] = new_sender_session
        sender_sessions[(current_chunkhash_str, ip, port)].timer = time.time()
        for i in range(sender_sessions[(current_chunkhash_str, ip, port)].window_size):
            left = (i) * MAX_PAYLOAD
            right = min((i + 1) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
            next_data = config.haschunks[current_chunkhash_str][left: right]
            # 发送下一节数据
            # TODO:
            data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN,
                                      HEADER_LEN + len(current_chunkhash_byte) + len(next_data),
                                      i + 1, 0)
            sock.sendto(data_header + current_chunkhash_byte + next_data, from_addr)
            sender_sessions[(current_chunkhash_str, ip, port)].time_record[i + 1] = time.time()
            sender_sessions[(current_chunkhash_str, ip, port)].next_seq += 1

            # print(i)
            # time.sleep(0.1)
    elif Type == 3:
        if not chunks_ok[current_chunkhash_str]:
            # 收到DATA
            # 如果当前序列号为Seq的包已经被收过了
            # print('received_chunks_data: ', received_chunks_data[current_chunkhash_str])
            # sock.add_log("rcv_chunkhash_str : {}".format(current_chunkhash_str))
            recv_time[current_chunkhash_str] = time.time()
            # if current_chunkhash_str not in received_chunks_data:
                # sock.add_log("wrong_chunkhash_str : {}".format(current_chunkhash_str))
                # received_chunks_data[current_chunkhash_str] = dict()
                # received_chunks_acks[current_chunkhash_str] = 1
            if Seq not in received_chunks_data[current_chunkhash_str]:
                # if (len(data) != 1024):
                    # sock.add_log("wrong1024 : {}".format(Seq))
                received_chunks_data[current_chunkhash_str][Seq] = data

            # 更新当前acknum(累计ack)
            sending_ack = received_chunks_acks[current_chunkhash_str]
            while sending_ack in received_chunks_data[current_chunkhash_str]:
                sending_ack += 1
            received_chunks_acks[current_chunkhash_str] = sending_ack

            # 发送ACK
            ack_pkt = struct.pack(PACKET_FORMAT, 52305, 35, 4,
                                  HEADER_LEN, HEADER_LEN + len(current_chunkhash_byte), Seq, sending_ack)
            sock.sendto(ack_pkt + current_chunkhash_byte, from_addr)

            # 判断当前chunk下载是否结束
            # 计算收到的总长度
            total_len = 0
            all_packets = received_chunks_data[current_chunkhash_str]

            for seq, packet_data in all_packets.items():
                total_len += len(packet_data)

            # sock.add_log("total_len : {}".format(total_len / 1024))
            # sock.add_log("data: {}".format(sorted(received_chunks_data[current_chunkhash_str].keys())))
            # sock.add_log("ing_final_received_chunks:{}".format(final_received_chunks.keys()))

            if total_len == CHUNK_DATA_SIZE:
                # sock.add_log("完成接受")
                # sock.add_log("chunk:{}".format(current_chunkhash_str))
                # 先按照seq顺序将data拼接好
                final_data = bytes()
                sorted_packets = sorted(received_chunks_data[current_chunkhash_str])
                for seq in sorted_packets:
                    final_data += received_chunks_data[current_chunkhash_str][seq]
                # 保存下载文件
                final_received_chunks[current_chunkhash_str] = final_data
                # sock.add_log("new_final_received_chunks:{}".format(final_received_chunks.keys()))
                chunks_ok[current_chunkhash_str] = True
                have_snd_all = True
                for chunk_str, ok in chunks_ok.items():
                    have_snd_all = have_snd_all and ok
                    if not have_snd_all:
                        break
                if have_snd_all:
                    # sock.add_log("zero?_final_received_chunks:{}".format(final_received_chunks.keys()))
                    with open(download_filenames[current_chunkhash_str], "wb") as wf:
                        pickle.dump(final_received_chunks, wf)
                    del final_received_chunks[current_chunkhash_str]

                # 将该文件加入到 haschunks中
                config.haschunks[current_chunkhash_str] = final_data

                # 下载完毕，清除已有chunkhash状态
                # del received_chunks_data[current_chunkhash_str]
                # del received_chunks_acks[current_chunkhash_str]
                received_chunks_data[current_chunkhash_str] = dict()
                received_chunks_acks[current_chunkhash_str] = 1
                del recv_time[current_chunkhash_str]

                # print(f"GOT {download_filenames[current_chunkhash_str]}")
                # del download_filenames[current_chunkhash_str]

                sha1 = hashlib.sha1()
                sha1.update(final_data)
                received_chunkhash_str = sha1.hexdigest()
                # sock.add_log("cal_hash : {}".format(received_chunkhash_str))
                # print(f"Expected chunkhash: {current_chunkhash_str}")
                # print(f"Received chunkhash: {received_chunkhash_str}")
                success = current_chunkhash_str == received_chunkhash_str
                # print(f"Successful received: {success}")
                # if success:
                #     print("Congrats! You have completed the example!")
                # else:
                #     print("Example fails. Please check the example files carefully.")
        else:
            # 发送ACK
            # sock.add_log("this file : {} have received all".format(current_chunkhash_str))
            ack_pkt = struct.pack(PACKET_FORMAT, 52305, 35, 4,
                                  HEADER_LEN, HEADER_LEN + len(current_chunkhash_byte), Seq, 513)
            sock.sendto(ack_pkt + current_chunkhash_byte, from_addr)
    elif Type == 4:
        # 收到ACK
        cur_session = sender_sessions[(current_chunkhash_str, ip, port)]
        receive_time = time.time()
        win_time[time.time()] = cur_session.window_size
        # 重新计算超时时间
        sampleRTT = receive_time - cur_session.time_record[Seq]
        cur_session.estimatedRTT = (1 - cur_session.alpha) * cur_session.estimatedRTT + \
                                   cur_session.alpha * sampleRTT
        cur_session.devRTT = (1 - cur_session.beta) * cur_session.devRTT + cur_session.beta * (
            abs(sampleRTT - cur_session.estimatedRTT))
        cur_session.timeout_interval = cur_session.estimatedRTT + 4 * cur_session.devRTT
        # sock.add_log("timeout : {}".format(cur_session.timeout_interval))

        if Seq > cur_session.base:
            cur_session.time_record[Seq] = time.time() + 10000

        ack_num = Ack
        # print('ack', ack_num)
        # print('base', cur_session.base)

        # sock.add_log('base: {}'.format(cur_session.base))
        # print('next_seq', cur_session.next_seq)
        if ack_num - cur_session.base >= cur_session.window_size:
            cur_session.base = ack_num
        else:
            for i in range(ack_num - cur_session.base, 0, -1):
                cur_session.window[i] = 1

        # cur_session.window[ack_num - cur_session.base] = 1
        # print(cur_session.window)
        # sock.add_log('cur_session.window: {}'.format(cur_session.window))
        if (ack_num - 1) * MAX_PAYLOAD >= CHUNK_DATA_SIZE:
            # 完成发送
            # print(f"finished sending {current_chunkhash_str}")
            cur_session.OK = True
            # 画图
            win_vis()
        else:
            if cur_session.window[1] == 1:
                # cur_session.timer = time.time()
                x = 0
                for i in range(1, len(cur_session.window)):  # 看看window的情况
                    if cur_session.window[i] == 1:
                        x += 1
                    else:
                        break
                if x == 0:  # 一个ack都还没收到
                    pass
                elif x == cur_session.window_size:  # ack满了
                    cur_session.base += cur_session.window_size
                    cur_session.window = [0] * (cur_session.window_size + 1)
                else:  # 0就先不用管这个ack了，但后面fast retransmission可能用到
                    right = copy.deepcopy(cur_session.window[x + 1:])
                    zeros = [0] * (cur_session.window_size - len(right) + 1)
                    cur_session.window = right + zeros
                    # print('right', right)
                    # print('zeros', zeros)
                    cur_session.base += x

            while cur_session.next_seq < cur_session.base + cur_session.window_size:
                if cur_session.next_seq > CHUNK_DATA_SIZE // MAX_PAYLOAD:
                    break
                left = (cur_session.next_seq - 1) * MAX_PAYLOAD
                right = min((cur_session.next_seq) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                next_data = config.haschunks[current_chunkhash_str][left: right]
                # print('left', left)
                # print('right', right)
                # if(cur_session.next_seq) == 512:
                #     print('512left',left)
                #     print('512right',right)
                # 发送下一节数据
                data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN,
                                          HEADER_LEN + len(current_chunkhash_byte) + len(next_data),
                                          cur_session.next_seq, 0)
                sock.sendto(data_header + current_chunkhash_byte + next_data, from_addr)
                cur_session.time_record[cur_session.next_seq] = time.time()
                # sock.add_log("nex_seq : {}".format(cur_session.next_seq))
                cur_session.next_seq += 1
                # if (cur_session.next_seq < cur_session.base + cur_session.window_size):
                #     left = (cur_session.next_seq-1) * MAX_PAYLOAD
                #     right = min((cur_session.next_seq) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                #     next_data = config.haschunks[current_chunkhash_str][left: right]
                #     # print('left', left)
                #     # print('right', right)
                #     # if(cur_session.next_seq) == 512:
                #     #     print('512left',left)
                #     #     print('512right',right)
                #     # 发送下一节数据
                #     data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN, HEADER_LEN + len(current_chunkhash_byte) + len(next_data),
                #                               Ack, 0)
                #     sock.sendto(data_header + current_chunkhash_byte + next_data, from_addr)
                if cur_session.base == cur_session.next_seq:
                    # cur_session.timer = time.time()  # start_timer
                    cur_session.next_seq += 1
                    # print('next_seq',cur_session.next_seq)

        # 判断是否为3个冗余ACK
        if duplicate_ACK.get(current_chunkhash_str).get(ack_num) is not None:
            num = duplicate_ACK.get(current_chunkhash_str).get(ack_num) + 1
            duplicate_ACK.get(current_chunkhash_str)[ack_num] = num
        else:
            duplicate_ACK.get(current_chunkhash_str)[ack_num] = 1

        # sock.add_log("how_many_ack : {}".format(duplicate_ACK.get(current_chunkhash_str).get(ack_num)))

        if duplicate_ACK.get(current_chunkhash_str).get(ack_num) >= 3:
            # sock.add_log("触发冗余重传")
            duplicate_ACK.get(current_chunkhash_str)[ack_num] = -1000
            left = (ack_num - 1) * MAX_PAYLOAD
            right = min((ack_num) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
            next_data = config.haschunks[current_chunkhash_str][left: right]
            data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN,
                                      HEADER_LEN + len(current_chunkhash_byte) + len(next_data),
                                      ack_num, 0)
            sock.sendto(data_header + current_chunkhash_byte + next_data, from_addr)
            # sock.add_log('ack_num: {}'.format(ack_num))
            cur_session.congestion_control(True)
        else:
            cur_session.congestion_control(False)


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
    global recv_time
    global received_chunks_acks
    global received_chunks_data
    global send_whohas_times
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

            # 超时重传
            for (chunkhash_str, ip, port), curr in sender_sessions.items():  # 这里将来要改
                if not curr.OK:
                    for seq, time_r in curr.time_record.items():
                        if seq >= curr.base:
                            time_cost = time.time() - time_r
                            # print(curr.next_seq)
                            if (time_cost > curr.timeout_interval):
                                # sock.add_log("pkt{}".format(seq) + "触发超时重传")
                                # sock.add_log("time : {}".format(time_cost))
                                # sock.add_log("timeout : {}".format(curr.timeout_interval))
                                curr.timeout_interval = 2.5  # 要改
                                curr.time_record[seq] = time.time()
                                left = (seq - 1) * MAX_PAYLOAD
                                right = min(seq * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                                next_data = config.haschunks[chunkhash_str][left: right]
                                chunkhash_byte = bytes.fromhex(chunkhash_str)
                                # 发送下一节数据
                                data_header = struct.pack(PACKET_FORMAT, 52305, 35, 3, HEADER_LEN,
                                                          HEADER_LEN + len(chunkhash_byte) + len(next_data),
                                                          seq, 0)
                                sock.sendto(data_header + chunkhash_byte + next_data, (ip, port))  ###这里将来要改
                                cur_session = sender_sessions[(chunkhash_str, ip, port)]
                                cur_session.congestion_control(True)

            # 100s内没有接收到新的包，就重新发送whohas请求
            for cur_chunkhash_str in list(recv_time.keys()):
                if time.time() - recv_time[cur_chunkhash_str] > 100:
                    # sock.add_log("重新请求100s " + cur_chunkhash_str)
                    del recv_time[cur_chunkhash_str]
                    # del received_chunks_data[cur_chunkhash_str]
                    # del received_chunks_acks[cur_chunkhash_str]
                    received_chunks_data[cur_chunkhash_str] = dict()
                    received_chunks_acks[cur_chunkhash_str] = 0

                    datahash = bytes.fromhex(cur_chunkhash_str)
                    whohas_header = struct.pack(
                        PACKET_FORMAT, 52305, 35, 0, HEADER_LEN, HEADER_LEN + len(datahash), 0, 0)
                    whohas_pkt = whohas_header + datahash + datahash

                    # 广播给所有的peer
                    peer_list = config.peers
                    for p in peer_list:
                        id, host, port = p
                        if int(id) != config.identity:
                            sock.sendto(whohas_pkt, (host, int(port)))

            # 20s内没有接受到IHAVE，就重新发送whohas
            for chunkhash_str in list(send_whohas_times.keys()):
                # sock.add_log(f"过了{time.time() - send_whohas_times[chunkhash_str]}s")
                t, flag = send_whohas_times[chunkhash_str]
                if not flag:
                    if time.time() - t > 20:
                        # sock.add_log("重新请求20s " + chunkhash_str)
                        # sock.add_log(f"{time.time() - send_whohas_times[chunkhash_str]}秒")
                        received_chunks_data[chunkhash_str] = dict()
                        received_chunks_acks[chunkhash_str] = 0

                        datahash = bytes.fromhex(chunkhash_str)
                        whohas_header = struct.pack(
                            PACKET_FORMAT, 52305, 35, 0, HEADER_LEN, HEADER_LEN + len(datahash), 0, 0)
                        whohas_pkt = whohas_header + datahash + datahash

                        # send_whohas_times[chunkhash_str] = time.time()

                        # 广播给所有的peer
                        peer_list = config.peers
                        for p in peer_list:
                            id, host, port = p
                            if int(id) != config.identity:
                                if chunkhash_str not in send_whohas_times:
                                    send_whohas_times[chunkhash_str] = (time.time(), False)
                                sock.sendto(whohas_pkt, (host, int(port)))

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
