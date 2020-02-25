#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import threading
import time
import zmq

import os
import sys
from kazoo.client import KazooClient

zk = KazooClient (hosts = "127.0.0.1:2181")



#topicPubIDDict=dict()
topicPubIpAddrDict=dict()
topicSubIpAddrDict=dict()
topicValueDict=dict()
Sema=threading.Semaphore(value=1)


def zmq_server_recv_pub_req_send_rsp_zk():
    
    port=5555

    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        #socket.bind("tcp://*:5555")
        socket.bind("tcp://127.0.0.1:5555")


        #Wait for next request from client
        message = socket.recv_string()
        print("Received pub_req: %s" % message)
        topic=message
        #pubID=message[1]

        #topicPubIDDict[topic]=pubID

        #  Do some 'work'
        #time.sleep(1)

        if topic in topicPubIpAddrDict:
            pass
        else:
            port=port+1
            msg='tcp://127.0.0.1:'+str(port)
            topicPubIpAddrDict[topic]=msg


        #start the new pub socket to recv
        #recv_pub_init(topic)
        #  Send reply back to client with new ip_addr_str
        recv_pub_and_send_sub_init(topic)
        #socket.send_string(msg)
        #write topic and its address_string in Zk
        topicStr="/PubTopic-"+topic
        bmsg=str.encode(msg)
        print("zk_pub_topic",topicStr)

        if zk.exists(topicStr):
            pass
        else:
            zk.create (topicStr, value=bmsg, ephemeral=True, makepath=True)
    
        #topicStr="/SubTopic-"+topic
        
        #bmsg=str.encode(msg)
        #zk.create (topicStr, value=bmsg, ephemeral=True, makepath=True)



        



#def zmq_server_recv_pub(addr_str):
#    context = zmq.Context()
#    socket = context.socket(zmq.REP)
#    socket.bind(addr_str)

    #  Wait for next request from client
#    message = socket.recv_string()
#    print("Received sub_req: %s" % message)
#    return message

#def zmq_server_send_sub(addr_str,data):
#    context = zmq.Context()
#    socket = context.socket(zmq.REQ)
#    socket.bind(addr_str)

    #  Wait for next request from client
#    socket.send_string(data)
#    print("send sub_req: %s" % data)
    


def zmq_server_recv_pub_and_send_sub(pub_addr_str,sub_addr_str):
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.DEALER)
    frontend.bind(pub_addr_str)
    backend.bind(sub_addr_str)

    # Initialize poll set
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    # Switch messages between sockets
    #while True:
    socks = dict(poller.poll())

    if socks.get(frontend) == zmq.POLLIN:
        message = frontend.recv_multipart()
        backend.send_multipart(message)

    if socks.get(backend) == zmq.POLLIN:
        message = backend.recv_multipart()
        frontend.send_multipart(message)





def zmq_server_recv_sub_req_send_rsp_zk():
    port=6666

    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        #socket.bind("tcp://*:6666")
        socket.bind("tcp://127.0.0.1:6666")
        

        #Wait for next request from client
        message = socket.recv_string()
        print("Received sub_req: %s" % message)
        topic=message
        #pubID=message[1]

        #topicPubIDDict[topic]=pubID
        #  Do some 'work'
        #time.sleep(1)
        

        if topic in topicSubIpAddrDict:
            msg=topicSubIpAddrDict[topic]
        else:
            port=port+1
            msg='tcp://127.0.0.1:'+str(port)
            topicSubIpAddrDict[topic]=msg

        #start the new pub socket to recv
        #send_sub_init(topic)
        #  Send reply back to client with new ip_addr_str
        
        #socket.send_string(msg)
        
        topicStr="/SubTopic-"+topic
        print("zk_sub_topic",topicStr)
        bmsg=str.encode(msg)
        if zk.exists(topicStr):
            pass
        else:
            zk.create (topicStr, value=bmsg, ephemeral=True, makepath=True)



# Set the Ip_addr and port of this broker in Zookeeper


def set_pub_broker_ip_addr_port_in_zk():
        print("ZK state :{}",zk.state)
        #if zk.exists("/activeBrokerPubAddr"):
        #    pass
        #else:
        zk.create ("/activeBrokerPubAddr", value=b"tcp://127.0.0.1:5555", ephemeral=True, makepath=True)


def set_sub_broker_ip_addr_port_in_zk():
        print("ZK state :{}",zk.state)
        #if zk.exists("/activeBrokerPubAddr"):
        #    pass
        #else:
        zk.create ("/activeBrokerSubAddr", value=b"tcp://127.0.0.1:6666", ephemeral=True, makepath=True)





    



# 
# Receive req_pub - Create thread 
#
def recv_req_pub_init():
    #set_pub_broker_ip_addr_port_in_zk()
    set_pub_broker_ip_addr_port_in_zk()
    threading.Thread(target=zmq_server_recv_pub_req_send_rsp_zk).start()
    #zmq_server_recv_pub_req_send_rsp()

def recv_req_sub_init():
    set_sub_broker_ip_addr_port_in_zk()
    threading.Thread(target=zmq_server_recv_sub_req_send_rsp_zk).start()


#def recv_pub_init(topic):
#    threading.Thread(target=recv_publish,args=[topic]).start()

#def send_sub_init(topic):
#    threading.Thread(target=send_subscribe,args=[topic]).start()

def recv_pub_and_send_sub_init(topic):
    threading.Thread(target=recv_publish_and_send_subscribe,args=[topic]).start()

    


def recv_publish_and_send_subscribe(topic):
    while(1):
        pub_addr_str=topicPubIpAddrDict[topic]

        sub_addr_str=topicSubIpAddrDict[topic]


        zmq_server_recv_pub_and_send_sub(pub_addr_str,sub_addr_str)


        #data=zmq_server_recv_pub(ip_addr_str)
        #    print("failed recv_publish")
        #    time.sleep(1)
        #topicValueDict[topic]=data
        time.sleep(1)
   



def recv_publish(topic):

    
    while(1):
        ip_addr_str=topicPubIpAddrDict[topic]
        



        #data=zmq_server_recv_pub(ip_addr_str)
        #    print("failed recv_publish")
        #    time.sleep(1)
        #topicValueDict[topic]=data
        time.sleep(1)


def send_subscribe(topic):
    while(1):
        try:
            

            ip_addr_str=topicSubIpAddrDict[topic]

            data=topicValueDict[topic]
            
            zmq_server_send_sub(ip_addr_str,data)
            del topicValueDict[topic]
            
            time.sleep(1)

        except Exception as e:
            #print("No data for topic\n")
            pass

        #time.sleep(1)
    


    


def main():
    print("*************Running BROKER ...\n***********")
    zk.start()
    recv_req_pub_init()

    recv_req_sub_init()
   


if __name__ == '__main__':
    main()
