import sys
sys.path.append('/home/rai/.virtualenvs/pyethapp/local/lib/python2.7/site-packages')
from gevent.event import AsyncResult
import gevent
import time
from eth_protocol import TransientBlockBody, TransientBlock
from ethereum.blocks import BlockHeader
from ethereum.slogging import get_logger
import ethereum.utils as utils
import traceback
from blinker import Signal
import Queue as Q
from gevent.queue import Queue

log = get_logger('eth.sync')
log_st = get_logger('eth.sync.task')
log_body = get_logger('eth.bodysync')
log_body_st = get_logger('eth.bodysync.task')

class HeaderRequest(object):

    def __init__(self, start=0, headers=[]):
        self.start = start
        self.headers = headers
        self.time = time.time()


class SyncTask(object):

    """
    synchronizes a the chain starting from a given blockhash
    blockchain hash is fetched from a single peer (which led to the unknown blockhash)
    blocks are fetched from the best peers

    with missing block:
        fetch headers
            until known block
    for headers
        fetch block bodies
            for each block body
                construct block
                chainservice.add_blocks() # blocks if queue is full
    """
    initial_blockheaders_per_request = 32
    max_blockheaders_per_request = 192
    max_skeleton_size = 128
    #max_blocks_per_request = 128
    max_blocks_per_request = 192
    max_retries = 5

    retry_delay = 1.
    blocks_request_timeout = 8.
    blockheaders_request_timeout = 15.
    

    def __init__(self, synchronizer, proto, blockhash, chain_difficulty=0, originator_only=False):
        self.synchronizer = synchronizer
        self.chain = synchronizer.chain
        self.chainservice = synchronizer.chainservice
        self.originating_proto = proto
        self.skeleton_peer = None 
        self.originator_only = originator_only
        self.blockhash = blockhash
        self.chain_difficulty = chain_difficulty
        self.requests = dict()  # proto: Event
        self.header_request = None
        self.header_processed = 0
        self.batch_requests = [] #batch header request 
        self.batch_result= [None]*self.max_skeleton_size*self.max_blockheaders_per_request
        self.headertask_queue = Q.PriorityQueue()
        self.pending_headerRequests = dict() 
        self.start_block_number = self.chain.head.number
        self.end_block_number = self.start_block_number + 1  # minimum synctask
        self.max_block_revert = 3600*24 / self.chainservice.config['eth']['block']['DIFF_ADJUSTMENT_CUTOFF']
        self.start_block_number_min = max(self.chain.head.number-self.max_block_revert, 0)
     #   self.blockheader_queue = Queue() #headers delivered for body download

   #     self.header_ready=signal('ready')
   #     self.header_ready.connect(self.deliver_header)
        gevent.spawn(self.run)

    def run(self):
        log_st.info('spawning new synctask')
        try:
            self.fetch_hashchain()
        except Exception:
            print(traceback.format_exc())
            self.exit(success=False)

    def exit(self, success=False):
        if not success:
            log_st.warn('syncing failed')
        else:
            log_st.debug('successfully synced')
        self.synchronizer.synctask_exited(success)

    @property
    def protocols(self):
        if self.originator_only:
            return [self.originating_proto]
        return self.synchronizer.protocols

    def fetch_hashchain(self):
       # local_blockhash=
       # from=commonancestor(self.blockhash,local_blockhash)
        skeleton = []
        header_batch = []
        skeleton_fetch=True 
        remaining=[]
        from0 = self.chain.head.number
         
        log_st.debug('current block number:%u', from0, origin=self.originating_proto)
        while True:
        # Get skeleton headers
            deferred = AsyncResult()

            if self.originating_proto.is_stopped:
            #    if protos:
            #        self.skeleton_peer= protos[0] 
            #    else: 
                   log_st.warn('originating_proto not available')
                   return self.exit(success=False)
            else:
                self.skeleton_peer=self.originating_proto  
            self.requests[self.skeleton_peer] = deferred 
            self.skeleton_peer.send_getblockheaders(from0+self.max_blockheaders_per_request,self.max_skeleton_size,self.max_blockheaders_per_request-1,0)  
            try:
                   skeleton = deferred.get(block=True,timeout=self.blockheaders_request_timeout)
             #      assert isinstance(skeleton,list)
                   log_st.debug('skeleton received %u',len(skeleton),skeleton=skeleton)  
            except gevent.Timeout:
                   log_st.warn('syncing skeleton timed out')
                 #todo drop originating proto
                   self.skeleton_peer.stop()

                   return self.exit(success=False)
            finally:
                #                    # is also executed 'on the way out' when any other clause of the try statement
                #                    # is left via a break, continue or return statement.
                #                    del self.requests[proto]
                   #del self.requests[self.originating_proto]
                   del self.requests[self.skeleton_peer]

                    
            #log_st.debug('skeleton received',num= len(skeleton), skeleton=skeleton)  
                   
            if skeleton_fetch and not skeleton:
               remaining = self.fetch_headers(self.skeleton_peer,from0)
                
               skeleton_fetch = False 
               if not remaining:
                  log_st.warn('no more skeleton received')
                  return self.exit(success=True)
            #  self.exit(success=False)
               #should not continuew??
             
            if skeleton_fetch:    
            
               self.fetch_headerbatch(from0,skeleton)
               log_st.debug('header batch', headerbatch= self.batch_result) 
              # check result
               if self.header_processed>0 :
         #          self.fetch_blocks(header_batch)
                   #from0 = from0 + self.max_skeleton_size*self.max_blockheaders_per_request   
                   from0 += self.header_processed
               else:
                   return self.exit(success=False)
          
            if remaining:
                log_st.debug('fetching new skeletons')
             #   self.fetch_blocks(remaining)
                from0+=len(remaining)
            




#send requests in batches, receive one header batch at a time    
    def fetch_headerbatch(self,origin,skeleton):
        log_st.debug('skeleton from',origin=origin)  
        
        # while True 
        self.header_processed = 0 
        #from0=skeleton[0]
        self.batch_requests=[]
        self.batch_result= [None]*self.max_skeleton_size*self.max_blockheaders_per_request
        headers= []
        proto = None
        proto_received=None #proto which delivered the header
        retry = 0
        received = False
        for header in skeleton:
            self.batch_requests.append(header)
            self.headertask_queue.put((header.number,header.number))           

        while True: 
         # requests = iter(self.batch_requests)  
          
          deferred = AsyncResult()
          self.header_request=deferred 
          
          #check timed out pending requests
          for proto in list(self.pending_headerRequests):
              request= self.pending_headerRequests[proto]
              if time.time()-request.time > self.blockheaders_request_timeout:
                  if request.start > 0:
                      self.headertask_queue.put((request.start,request.start))
                      log_st.debug('timeouted request',
                              start=request.start,proto=proto)
                      del self.pending_headerRequests[proto]
                 #     if overtimes> 2 else set it idle try one more time, 
                 #     proto.idle = True   
                      proto.stop()
          
          log_st.debug('header task queue size, pending queue size, batch_requestsize',size=self.headertask_queue.qsize(),pending=len(self.pending_headerRequests),batch_request=len(self.batch_requests)) 
          #if self.headertask_queue.qsize == 0 and len(self.pending_headerRequests)==0 and len(self.batch_requests)==0 :
          if len(self.batch_requests) == 0:
               log_st.debug('batch header fetching completed!')
               return self.batch_result
           
          fetching = False 
          task_empty = False
          pending=len(self.pending_headerRequests)
          for proto in self.idle_protocols():
             
             proto_deferred = AsyncResult()
               # check if it's finished
              
             if not self.headertask_queue.empty():
                 start=self.headertask_queue.get()[1] 
                 self.requests[proto] = proto_deferred
                 proto.send_getblockheaders(start,self.max_blockheaders_per_request)
                 self.pending_headerRequests[proto] = HeaderRequest(start) 
                 proto.idle = False 
                 fetching = True
                 log_st.debug('sent header request',request= start , proto=proto)
             else:
                 task_empty= True
                 break
          # check if there are protos available for header fetching 
          #if not fetching and not self.headertask_queue.empty() and pending == len(self.pending_headerRequests):
          if not fetching and not task_empty:
                log_st.warn('no protocols available')
                return self.exit(success=False) 
          elif task_empty and pending==len(self.pending_headerRequests):
                continue
          try:
               proto_received = deferred.get(timeout=self.blockheaders_request_timeout)['proto']
               header=deferred.get(timeout=self.blockheaders_request_timeout)['headers']
               log_st.debug('headers batch received from proto', header=header)
          except gevent.Timeout:
               log_st.warn('syncing batch hashchain timed out')
               retry += 1
               if retry >= self.max_retries:
                    log_st.warn('headers sync failed with all peers',
                            num_protos=len(self.idle_protocols()))
                    return self.exit(success=False)
               else:
                    log_st.info('headers sync failed with peers, retry', retry=retry)
                    gevent.sleep(self.retry_delay)
               continue
          finally:
               del self.header_request 
            
          # check if header is empty  

          if header[0] not in self.batch_requests:
             continue
          if proto_received not in self.pending_headerRequests:
              continue
          start_header= self.pending_headerRequests[proto_received].start 
          del self.pending_headerRequests[proto_received] 
          verified = self.verify_headers(proto_received,header)
          if not verified:
             self.headertask_queue.put((start_header,start_header))
             continue

           
          batch_header= header[::-1] #in hight rising order
          self.batch_result[(batch_header[0].number-origin-1):batch_header[0].number-origin-1+len(batch_header)]= batch_header
         # log_st.debug('batch result',batch_result= self.batch_result) 
          self.batch_requests.remove(header[0])
          proto_received.set_idle()
          del self.requests[proto_received] 
          
          header_ready = 0
          while (self.header_processed + header_ready) < len(self.batch_result) and self.batch_result[self.header_processed + header_ready]:
              header_ready += self.max_blockheaders_per_request

          if header_ready > 0 :
             # Headers are ready for delivery, gather them
             processed = self.batch_result[self.header_processed:self.header_processed+header_ready]
          #   log_st.debug('issue fetching blocks',header_processed=self.header_processed, blocks=processed, proto=proto_received,count=len(processed),start=processed[0].number)  
             
             count=len(processed)
             self.synchronizer.blockheader_queue.put(processed)
           # if self.fetch_blocks(processed):                            
             self.header_processed += count 
             #else:
             #    return self.batch_result[:self.header_processed] 
          log_st.debug('remaining headers',num=len(self.batch_requests),headers=self.batch_requests)
          
                 


    def idle_protocols(self):
        idle = []
        for proto in self.protocols:
            if proto.idle:
               idle.append(proto)
        return idle        


    def verify_headers(self,proto,headers):
 #       start = self.pending_headerRequests[proto]
 #       headerhash= self.batch_requests[start].hash
 #       if headers[0].number != start:
 #          log_st.warn('First header broke chain ordering', proto=proto,number =headers[0].number,headerhash = headers[0].hash,start=start)  
 #          return False
 #       elif headers[len(headers)-1].hash != target  

       # if request:
       #    return 0, errNoFetchesPending
        if len(headers) != self.max_blocks_per_request:
            log_st.debug('headers batch count', count=len(headers))  
            
            return False

        return True       

   
    def fetch_headers(self,proto, fromx):
        deferred = AsyncResult()
        blockheaders_batch=[]
        proto.send_getblockheaders(fromx,self.max_blockheaders_per_request)
        try:
            blockheaders_batch = deferred.get(block=True,timeout=self.blockheaders_request_timeout)
        except gevent.Timeout:
            log_st.warn('syncing batch hashchain timed out')
            return []
        finally:
            return blockheaders_batch
        

    def receive_blockheaders(self, proto, blockheaders):
        log.debug('blockheaders received:', proto=proto, num=len(blockheaders), blockheaders=blockheaders)
        if proto not in self.requests:
           log.debug('unexpected blockheaders')
           return
        if self.batch_requests:
            self.header_request.set({'proto':proto,'headers':blockheaders})
        elif proto == self.skeleton_peer: #make sure it's from the originating proto 
            self.requests[proto].set(blockheaders)
         

class SyncBody(object):

    max_blocks_per_request = 128
    max_blocks_process= 2048 
    blocks_request_timeout = 8.
    max_retries = 5
    retry_delay = 3.
    def __init__(self, synchronizer, blockhash, chain_difficulty=0, originator_only=False):
        self.synchronizer = synchronizer
        self.chain = synchronizer.chain
      #  self.blockhash = blockhash
        self.chainservice = synchronizer.chainservice
        self.originator_only = originator_only
        self.chain_difficulty = chain_difficulty
        self.block_requests_pool = []
        self.bodytask_queue = Q.PriorityQueue()
        self.pending_bodyRequests = dict() 
        self.requests = dict()  # proto: Event
        self.body_request = None
        self.fetch_ready= AsyncResult()
        gevent.spawn(self.run)
   #     gevent.spawn(self.schedule_block_fetch) 

    @property
    def protocols(self):
        if self.originator_only:
            return [self.originating_proto]
        return self.synchronizer.protocols

    def exit(self, success=False):
        if not success:
            log_body_st.warn('body syncing failed')
        else:
            log_body_st.debug('successfully synced')
        self.synchronizer.syncbody_exited(success)


     
    def run(self):
        log_body_st.info('spawning new syncbodytask')

        try:
            gevent.spawn(self.schedule_block_fetch)
            gevent.spawn(self.fetch_blocks)
        except Exception:
            print(traceback.format_exc())
            self.exit(success=False)


    def schedule_block_fetch(self):
        batch_header = []
        log_st.debug('start sheduleing blocks')
        self.synchronizer.blockheader_queue = Queue() 
        
        while True:
          batch_header= self.synchronizer.blockheader_queue.get()
          num_blocks = len(batch_header)
          log_body_st.debug('delivered headers', delivered_heades=batch_header)
          while batch_header: 
            limit = len(batch_header) if len(batch_header) < self.max_blocks_process else self.max_blocks_process
            blockbody_batch = batch_header[:limit]
            for header in blockbody_batch:
                #check chain order 
                #self.block_requests_pool[header.hash]= header
                self.block_requests_pool.append(header)
                self.bodytask_queue.put((header.number,header))
            #check if length block_requests_pool is equal to blockhashes_batch
            
            batch_header = batch_header[limit:]     
          self.fetch_ready.set() 
      #    try:
      #       gevent.spawn(self.fetch_blocks)
      #    except Exception:
      #       self.exit(success=False) 

    def fetch_blocks(self):
        # fetch blocks (no parallelism here)
      #  log_st.debug('fetching blocks', num=len(blockheaders_chain))
      #  assert blockheaders_chain
       # blockheaders_chain.reverse()  # height rising order
        num_blocks = 0
        num_fetched = 0
        retry = 0
        last_block = None 
#        batch_header = []
        #while not self.blockheaders_queue.empty():
#        self.synchronizer.blockheader_queue = Queue() 
#        while True:
#          batch_header= self.synchronizer.blockheader_queue.get()
#          num_blocks = len(batch_header)
#          log_body_st.debug('delivered headers', delivered_heades=batch_header)
#          gevent.sleep(0)
#          while batch_header: 
#            limit = len(batch_header) if len(batch_header) < self.max_blocks_process else self.max_blocks_process
#            blockbody_batch = batch_header[:limit]
#            for header in blockbody_batch:
                #check chain order 
                #self.block_requests_pool[header.hash]= header
#                self.block_requests_pool.append(header)
#                self.bodytask_queue.put((header.number,header))
            #check if length block_requests_pool is equal to blockhashes_batch
#            batch_header = batch_header[limit:]     

        while True:
          try:
                result = self.fetch_ready.get()
                log_st.debug('start fetching blocks')
                deferred = AsyncResult()
                self.body_request=deferred 
              
              #check timed out pending requests
                for proto in list(self.pending_bodyRequests):
                    request= self.pending_bodyRequests[proto]
                    if time.time()-request.time > self.blocks_request_timeout:
                      if request.start >= 0:
                          for h in request.headers:
                              self.bodytask_queue.put((h.number,h))
                          log_body_st.debug('timeouted request',
                                  start=request.start,proto=proto)
                          del self.pending_bodyRequests[proto]
                     #     if overtimes> 2 else set it idle try one more time, 
                     #     proto.body_idle = True   
                          proto.stop()
              
             #   log_st.debug('header task queue size, pending queue size, batch_requestsize',size=self.bodytask_queue.qsize(),pending=len(self.pending_blockRequests),batch_request=len(self.block_requests_pool)) 
              #if self.headertask_queue.qsize == 0 and len(self.pending_headerRequests)==0 and len(self.batch_requests)==0 :
                if len(self.block_requests_pool) == 0:
                   log_body_st.debug('block body fetching completed!')
                  # return True
                   break
               
                fetching = False 
                task_empty = False
                pending=len(self.pending_bodyRequests)
                for proto in self.body_idle_protocols():
               #    assert proto not in self.requests
                   if proto.is_stopped:
                        continue
                   proto_deferred = AsyncResult()
                   # check if it's finished
                   block_batch=[]
                   if not self.bodytask_queue.empty():
                     log_body_st.debug('pending body queue size', pending=
                             self.bodytask_queue.qsize() )
                     while len(block_batch)<self.max_blocks_per_request and not self.bodytask_queue.empty():
                       blockheader=self.bodytask_queue.get()[1]
                       block_batch.append(blockheader)
                   
                     self.requests[proto] = proto_deferred
                     blockhashes_batch = [h.hash for h in block_batch]
                     log_body_st.debug('requesting blocks',
                             num=len(blockhashes_batch),missing = self.bodytask_queue.qsize()-len(blockhashes_batch))
                     proto.send_getblockbodies(*blockhashes_batch)
                     self.pending_bodyRequests[proto] = HeaderRequest(block_batch[0].number, block_batch) 
                     proto.body_idle = False 
                     fetching = True
                    # start =+ self.max_blocks_per_request
                   else:
                     task_empty= True
                     break
              # check if there are protos available for header fetching 
              #if not fetching and not self.headertask_queue.empty() and pending == len(self.pending_headerRequests):
                if not fetching and not task_empty:
                     log_body_st.warn('no protocols available')
                     return self.exit(success=False) 
                elif task_empty and pending==len(self.pending_bodyRequests):
                     continue
                try:
                      proto_received = deferred.get(timeout=self.blocks_request_timeout)['proto']
                      bodies = deferred.get(timeout=self.blocks_request_timeout)['blocks']
                   #   log_st.debug('headers batch received from proto', header=header)
                except gevent.Timeout:
                      log_body_st.warn('syncing batch block body timed out')
                      retry += 1
                      if retry >= self.max_retries:
                        log_body_st.warn('headers sync failed with all peers',
                                num_protos=len(self.body_idle_protocols()))
                        return self.exit(success=False)
                      else:
                        log_body_st.info('headers sync failed with peers, retry', retry=retry)
                        gevent.sleep(self.retry_delay)
                      continue
                finally:
                      del self.body_request 
                
              # check if header is empty  

                if proto_received not in self.pending_bodyRequests:
                       continue
                headers = self.pending_bodyRequests[proto_received].headers 
                log_body_st.info('received body headers',
                        headernum=self.pending_bodyRequests[proto_received].start,
                        bodyrequest=self.pending_bodyRequests[proto_received].headers)
                del self.pending_bodyRequests[proto_received] 
                if not bodies:
                       log_st.warn('empty getblockbodies reply, trying next proto')
                       continue
                elif not isinstance(bodies[0], TransientBlockBody):
                       log_st.warn('received unexpected data')
                       bodies = []
                       continue

                # add received t_blocks
                num_fetched += len(bodies)
                log_body_st.debug('received block bodies',
                        num=len(bodies),blockbody=bodies,num_fetched=num_fetched,
                             total=num_blocks, missing=num_blocks - num_fetched)
                proto_received.body_idle=True                                                                      
                del self.requests[proto_received]
                ts = time.time()
                log_body_st.debug('adding blocks', qsize=self.chainservice.block_queue.qsize())
                index = 0 
                for h in headers:
                      try:
                        body = bodies[index]
                        index = index + 1
                        t_block = TransientBlock(h, body.transactions, body.uncles)
                        self.chainservice.add_block(t_block, proto)  # this blocks if the queue is full
                      except IndexError as e:
                        log_body_st.error('headers and bodies mismatch', error=e)
                        return self.exit(success=False)
                      self.block_requests_pool.remove(h) 
       
                log_body_st.debug('adding blocks done', took=time.time() - ts)
                
          except Exception as ex:
              log_body_st.error(error = ex)

                

        #        for body in bodies:
        #              try:
        #                h = headers.pop(0)
        #                t_block = TransientBlock(h, body.transactions, body.uncles)
        #                self.chainservice.add_block(t_block, proto)  # this blocks if the queue is full
        #                self.block_requests_pool.remove(h) 
        #              except IndexError as e:
        #                log_st.error('headers and bodies mismatch', error=e)
        #                self.exit(success=False)
        #        log_st.debug('adding blocks done', took=time.time() - ts)

                 # done
        last_block = t_block
      #  assert not len(batch_header)
        #  assert last_block.header.hash == self.blockhash
        log_body_st.debug('syncing finished')
        # at this point blocks are not in the chain yet, but in the add_block queue
        if self.chain_difficulty >= self.chain.head.chain_difficulty():
         self.chainservice.broadcast_newblock(last_block, self.chain_difficulty, origin=proto)
        self.exit(success = True)
          #return True
    
    def receive_blockbodies(self, proto, bodies):
        log.debug('block bodies received', proto=proto, num=len(bodies))
        if proto not in self.requests:
            log.debug('unexpected blocks')
            return
        self.body_request.set({'proto':proto,'blocks':bodies})

    def body_idle_protocols(self):
        idle = []
        for proto in self.protocols:
            if proto.body_idle:
               idle.append(proto)
        return idle        




class Synchronizer(object):

    """
    handles the synchronization of blocks

    there is only one synctask active at a time
    in order to deal with the worst case of initially syncing the wrong chain,
        a checkpoint blockhash can be specified and synced via force_sync

    received blocks are given to chainservice.add_block
    which has a fixed size queue, the synchronization blocks if the queue is full

    on_status:
        if peer.head.chain_difficulty > chain.head.chain_difficulty
            fetch peer.head and handle as newblock
    on_newblock:
        if block.parent:
            add
        else:
            sync
    on_blocks/on_blockhashes:
        if synctask:
            handle to requester
        elif unknown and has parent:
            add to chain
        else:
            drop
    """

    MAX_NEWBLOCK_AGE = 5  # maximum age (in blocks) of blocks received as newblock

    def __init__(self, chainservice, force_sync=None):
        """
        @param: force_sync None or tuple(blockhash, chain_difficulty)
                helper for long initial syncs to get on the right chain
                used with first status_received
        """
        self.chainservice = chainservice
        self.force_sync = force_sync
        self.chain = chainservice.chain
        self._protocols = dict()  # proto: chain_difficulty
        self.synctask = None
        self.syncbody = None
        self.blockheader_queue = Queue() 

    def synctask_exited(self, success=False):
        # note: synctask broadcasts best block
        if success:
            self.force_sync = None
        self.synctask = None
        if self.syncbody:
           self.syncbody = None 

    def syncbody_exited(self, success=False):
        # note: synctask broadcasts best block
        if success:
            self.force_sync = None
        self.syncbody = None
        if self.synctask:
            self.synctask = None


    @property
    def protocols(self):
        "return protocols which are not stopped sorted by highest chain_difficulty"
        # filter and cleanup
        self._protocols = dict((p, cd) for p, cd in self._protocols.items() if not p.is_stopped)
        return sorted(self._protocols.keys(), key=lambda p: self._protocols[p], reverse=True)


    def receive_newblock(self, proto, t_block, chain_difficulty):
        "called if there's a newblock announced on the network"
        log.debug('newblock', proto=proto, block=t_block, chain_difficulty=chain_difficulty,
                  client=proto.peer.remote_client_version)

        if t_block.header.hash in self.chain:
            assert chain_difficulty == self.chain.get(t_block.header.hash).chain_difficulty()

        # memorize proto with difficulty
        self._protocols[proto] = chain_difficulty

        if self.chainservice.knows_block(block_hash=t_block.header.hash):
            log.debug('known block')
            return

        # check pow
        if not t_block.header.check_pow():
            log.warn('check pow failed, should ban!')
            return

        expected_difficulty = self.chain.head.chain_difficulty() + t_block.header.difficulty
        if chain_difficulty >= self.chain.head.chain_difficulty():
            # broadcast duplicates filtering is done in eth_service
            log.debug('sufficient difficulty, broadcasting',
                      client=proto.peer.remote_client_version)
            self.chainservice.broadcast_newblock(t_block, chain_difficulty, origin=proto)
        else:
            # any criteria for which blocks/chains not to add?
            age = self.chain.head.number - t_block.header.number
            log.debug('low difficulty', client=proto.peer.remote_client_version,
                      chain_difficulty=chain_difficulty, expected_difficulty=expected_difficulty,
                      block_age=age)
            if age > self.MAX_NEWBLOCK_AGE:
                log.debug('newblock is too old, not adding', block_age=age,
                          max_age=self.MAX_NEWBLOCK_AGE)
                return

        # unknown and pow check and highest difficulty

        # check if we have parent
        if self.chainservice.knows_block(block_hash=t_block.header.prevhash):
            log.debug('adding block')
            self.chainservice.add_block(t_block, proto)
        else:
            log.debug('missing parent for new block', block=t_block)
            if not self.synctask:
                self.synctask = SyncTask(self, proto, t_block.header.hash, chain_difficulty)
                if not self.syncbody:
                  self.syncbody = SyncBody(self, chain_difficulty)
            else:
                log.debug('already syncing, won\'t start new sync task')

    def receive_status(self, proto, blockhash, chain_difficulty):
        "called if a new peer is connected"
        log.debug('status received', proto=proto, chain_difficulty=chain_difficulty)

        # memorize proto with difficulty
        self._protocols[proto] = chain_difficulty

        if self.chainservice.knows_block(blockhash) or self.synctask:
            log.debug('existing task or known hash, discarding')
            return

        if self.force_sync:
            blockhash, chain_difficulty = self.force_sync
            log.debug('starting forced syctask', blockhash=blockhash.encode('hex'))
            self.synctask = SyncTask(self, proto, blockhash, chain_difficulty)

        elif chain_difficulty > self.chain.head.chain_difficulty():
            log.debug('sufficient difficulty')
            self.synctask = SyncTask(self, proto, blockhash, chain_difficulty)
            if not self.syncbody:
              self.syncbody = SyncBody(self, chain_difficulty)
            
    def receive_newblockhashes(self, proto, newblockhashes):
        """
        no way to check if this really an interesting block at this point.
        might lead to an amplification attack, need to track this proto and judge usefullness
        """
        log.debug('received newblockhashes', num=len(newblockhashes), proto=proto)
        # log.debug('DISABLED')
        # return
        newblockhashes = [h.hash for h in newblockhashes if not self.chainservice.knows_block(h.hash)]
        if (proto not in self.protocols) or (not newblockhashes) or self.synctask:
            log.debug('discarding', known=bool(not newblockhashes), synctask=bool(self.synctask))
            return
        if len(newblockhashes) != 1:
            log.warn('supporting only one newblockhash', num=len(newblockhashes))
        if not self.chainservice.is_syncing:
            blockhash = newblockhashes[0]
            log.debug('starting synctask for newblockhashes', blockhash=blockhash.encode('hex'))
            self.synctask = SyncTask(self, proto, blockhash, 0, originator_only=True)
            self.syncbody = SyncBody(self, chain_difficulty, blockhash)



    def receive_blockbodies(self, proto, bodies):
        log.debug('blockbodies received', proto=proto, num=len(bodies))
        if self.syncbody:
            self.syncbody.receive_blockbodies(proto, bodies)
        else:
            log.warn('no synctask, not expecting block bodies')

    def receive_blockheaders(self, proto, blockheaders):
        log.debug('blockheaders received', proto=proto, num=len(blockheaders))
        if self.synctask:
            self.synctask.receive_blockheaders(proto, blockheaders)
        else:
            log.warn('no synctask, not expecting blockheaders')
