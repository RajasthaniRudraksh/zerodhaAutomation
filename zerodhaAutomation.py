import socket,threading
import json,pathlib,time,datetime
import radheUtils,xlwings,pickle,re

SERVER='127.0.0.1'
PORT=10000
ADDR=(SERVER,PORT)
HEADER=10
processId=1
processLock=threading.Lock()
sendLock=threading.Lock()
ReceivedData=[]
receiveEvent=threading.Event()
processedData={}
tokenDict={}
isTickerConnected=0
client=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
try:
    print(client.connect(ADDR))
except ConnectionRefusedError:
    print("Server is not running/down")
    exit()

def send(jsonData,usedProcessId=0):
    try:
        sendLock.acquire()
        returnData=None
        global processId
        if type(jsonData)==dict:
            if usedProcessId==0:
                processLock.acquire()
                temp=processId
                processId=processId+1
                processLock.release()
            else:
                temp=usedProcessId
            jsonData['processId']=temp
            jsonData=json.dumps(jsonData)
            radheUtils.advanceSend(client,jsonData,HEADER)
            returnData=temp
        else:
            print(f'{jsonData} = Json data can be sent only')
            returnData=None
    except Exception as e:
        print(e)
        returnData=None
    finally:
        sendLock.release()
        return returnData
    

def receive():
    global ReceivedData
    while True:
        data=radheUtils.advanceReceive(client)
        if data==b'':
            print("Receiver Closed")
            break
        else:
            ReceivedData.append(data)
            receiveEvent.set()
                    
def receiveHandler():
    global ReceivedData, isTickerConnected, tokenDict
    while True:
        try:
            popped=ReceivedData.pop(0)
            # print(popped)
        except:
            receiveEvent.clear()
            receiveEvent.wait(10)
            continue
        data=json.loads(popped)        
        if type(data)==dict:
            temp=data.get('processId')
            if temp==0:
                # print(data)
                isTickerConnected=data.get('flag')
                tokenDict=data.get('tickData')
                # print(tokenDict)
            elif temp>0 or temp==-1: 
                processedData[temp]=data
            else:
                print(f"Unexpected data {data}")
        else:
            print(f'Json Data Expected {data}')
        
threading.Thread(target=receive).start()
threading.Thread(target=receiveHandler).start()
print("Connected to local Server...")
####################################################################
#Connected with server and everything is ready

location=pathlib.Path(__file__).parent
excelFileName='AUTOMATIC TRADE SETUP.xlsm'
excelSheetName='Option'
excelFileLocation=location.joinpath(excelFileName)
processLists={}
rowNoQueue=[]
outputQueue=[]
startExcelPointer=7
wb=xlwings.Book(excelFileLocation)
ws=wb.sheets[excelSheetName]
instrumentsFile=location.joinpath('instrument.txt')
print("Loading Instrumets File...")

with open(instrumentsFile,'rb') as fp:
	instruments=pickle.load(fp)

print("Instruments file loaded successfully")

excelRef={
    'command':'P',
    'baseLTP':'F',
    'baseExchange':'G',
    'serial':'D',
    'userId':'E',
    'tradingSymbol':'K',
    'exchange':'L',
    'transaction_type':'N',
    'product':'M',
    'quantity':'O',
    'triggerPrice':'H',
    'condition':'I',
    'candleSize':'J',
}

excelOutputRef={
    # 'tradedPrice':'V',
    # 'OpenPosition':'W',
    # 'OpenPositionType':'X', 
    'response':'Q',
    'orderId':'S',
    'message':'R',
}


def excelRowValidation(item):
    result={}
    result['status']=0   
    if item.get('userId')==None or item.get('baseLTP')==None or item.get('tradingSymbol')==None:
        result['msg']=f'Invalid Data. UserId/BaseInstrument/Trading Symbol column can\'t be empty'
    elif item.get('transaction_type') not in ['BUY','SELL']:
        result['msg']=f'Invalid Data. BUY/SELL Keyword is expected in {excelRef.get("transaction_type")} Column'
    elif item.get('baseExchange') not in ['NFO','NSE','BSE','MCX'] or item.get('exchange') not in ['NFO','NSE','BSE','MCX']:
        result['msg']=f'Invalid Data. Following Exchange Keywords are expected. (BSE,NSE,MCX,NFO)'
    elif ( type(item.get('quantity')) not in [int,float] ) or ( type(item.get('triggerPrice')) not in [int,float] ) or ( type(item.get('candleSize')) not in [int,float] ):
        result['msg']='Invalid Data. All these columns must contain numeric value. (Quantity, Trigger Price, Candle Size)'        
    elif item.get('condition') not in ['ABOVE','BELOW']:
        result['msg']=f'Invalid Data. Following Keywords are expected in Condition Column. (ABOVE, BELOW)'
    elif item.get('product') not in ['NRML','MIS','CNC']:
        result['msg']=f'Invalid Data. Following Keywords are expected in Product Column. (NRML, MIS, CNC)'
    elif item.get('candleSize')<1:
        result['msg']=f'Candle Size must be greater than or equal to 1 minute'
    else:
        result['status']=1
    return result
        
        
  
def orderDecoder():
    wb1=xlwings.Book(excelFileLocation)
    ws1=wb1.sheets[excelSheetName]
    while True:
        try:
            find=0
            popped=None
            invalidDataFlag=0
            try:
                popped=rowNoQueue.pop(0)
            except:
                time.sleep(1)
                continue
            
            command=popped[1]
            if command in ['0',0,'c']:
                print(f"c command received from row no {popped[0]}")
                output={}
                for i in excelOutputRef.values():
                    output[i]=''
                outputQueue.append({'excelRowId':popped[0],'data':output})  
                
                
                #Copy the row from excel
                item={}
                item['excelRowId']=popped[0]
                for i in excelRef.keys():
                    item[f'{i}']=radheUtils.upp(ws1.range(f'{excelRef.get(i)}{popped[0]}').value)    
                
                item['order_type']='MARKET'
                item['variety']='regular'
                item['validity']='DAY'
                item['command']=command

                    
                result=excelRowValidation(item)
                if result.get('status')==0:
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Invalid Data',excelOutputRef.get('message'):result.get('msg')}})
                    continue
                    
                result=radheUtils.search(instruments,item.get('tradingSymbol'),item.get('exchange'))
                result2=radheUtils.search(instruments,item.get('baseLTP'),item.get('baseExchange'))
                
                print(result)
                if result==0 or result2==0:
                    invalidDataFlag=1
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Invalid Data',excelOutputRef.get('message'):'Instrument is invalid'}})
                    continue
                else:
                    item['instrument_token']=result
                    item['baseToken']=result2
                item['candleSize']=int(item.get('candleSize')) #Eliminating the round off part if any present in candleSize
                print(item)                  
                #Killing if any previous thread running
                rowId=item.get('excelRowId')
                if processLists.get(item.get('excelRowId'))!=None:
                    print(f"Killing the existing thread associated with {rowId}")
                    processLists[rowId]['stopFlag2']=True
                    processLists[rowId]['stopFlag']=True
                    if processLists[rowId].get('wait')!=None:
                        try:
                            processLists[rowId].get('wait').set()
                        except:
                            pass
                    processLists.pop(rowId)
                processLists[rowId]={}
                processLists[rowId]['stopFlag']=False
                processLists[rowId]['stopFlag2']=False
                print(processLists)
                copyItem=item.copy()
                threading.Thread(target=priceOrder, args=(copyItem,processLists.get(rowId))).start()

            elif command in ['9',9,'x']:
                print(f"x/stop command received from row no {popped[0]}")
                try:
                    dictObject=processLists[popped[0]]
                    dictObject['stopFlag2']=True
                    dictObject['stopFlag']=True
                    if dictObject.get('wait')!=None:
                        dictObject.get('wait').set()
                    processLists.pop(popped[0])
                    outputQueue.append({'excelRowId':popped[0],'data':{excelOutputRef.get('response'):'Stopping Thread',excelOutputRef.get('message'):'Stopping The Thread'}})
                except Exception as e:
                    outputQueue.append({'excelRowId':popped[0],'data':{excelOutputRef.get('response'):'No Active Thread'}})
        except Exception as e:
            print(e)

def nextFrame(seconds,start=None):
    # seconds=minute*60
    if start==None:
        start=datetime.datetime.now()
        start=start.replace(hour=9,minute=15,second=0)
    now=datetime.datetime.now()
    diff=now-start
    
    result=round(diff.total_seconds()%seconds) #Removing miliseconds 
    result=seconds-result
    return result
    # resultTime=now + datetime.timedelta(seconds=result)
    # return resultTime

def convertToSeconds(string):
    textC=re.compile(r'[a-z,A-Z]+')
    timeC=re.compile(r'^\d+')
    timeAll=timeC.findall(string)
    textAll=textC.findall(string)
    if timeAll==[] or textAll==[]:
        return {'status':0}
    time=int(timeAll[0])
    unit=textAll[0].lower()
    multiply=0
    if unit[0]=='m':
        multiply=60
    elif unit[0]=='h':
        multiply=3600

    if multiply==0:
        return {'status':0}
    time=time*multiply
    return {'status':1,'seconds':time}

def priceOrder(item,dictObject):
    if isTickerConnected:
        def hi():
            pass

        def baseFunc():
            return tokenDict.get(item.get('baseToken')).get('ltp')

        subscribe(item.get('baseToken'))
        item['buyLtpFunction']=baseFunc
        item['sellLtpFunction']=baseFunc        
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Waiting',excelOutputRef.get('message'):'Waiting for Condition to Meet'}})        
        priceOrderHeart(item,dictObject,item.get('condition'))
        if dictObject.get('stopFlag'):
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stopped',excelOutputRef.get('message'):'Stopped Before Trade'}})
            return 0
        print(f'Condition Meet Placing An Order {item.get("excelRowId")}')
        
        result=placeOrderToLocalServer(item,0)
        print(result)
        if result.get('status')==1:
            if result.get('orderStatus')=='COMPLETE':
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Done',excelOutputRef.get('orderId'):result.get('orderId'),excelOutputRef.get('message'):'Order Successful Completed'}})
                return 0        
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error', excelOutputRef.get('orderId'):result.get('orderId'), excelOutputRef.get('message'):f'{result.get("msg")}'}})
    else:
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Not Connected To Ticker'}})                
        print("Not Connected to ticker")


def priceOrderHeart(item,dictObject,transaction):
    def trueFunc():
        pass
    candle=item.get('candleSize')*60
    dictObject['wait']=threading.Event()
    if transaction=='ABOVE':
        print("I am Above")
        def buyCon():
            second=nextFrame(candle)
            dictObject.get('wait').wait(second)
            if dictObject.get('wait').is_set():
                return dictObject.get('stopFlag')
            ltp=item.get('buyLtpFunction')()#tokenDict.get(item.get('instrument_token')).get('ltp')
            print(f"offer price {ltp}")
            return ltp>=item.get('triggerPrice') or dictObject.get('stopFlag')
            # if second>2:
            #     dictObject.get('wait').wait(second-2)
            #     if dictObject.get('wait').is_set():
            #         return dictObject.get('stopFlag')
            # else:
            #     ltp=item.get('buyLtpFunction')()#tokenDict.get(item.get('instrument_token')).get('ltp')
            #     print(f"offer price {ltp}")
            #     return ltp>=item.get('triggerPrice') or dictObject.get('stopFlag')
        radheUtils.conditionStopper(buyCon,trueFunc)
    elif transaction=='BELOW':
        print('I am Below')
        def sellCon():
            second=nextFrame(candle)
            dictObject.get('wait').wait(second)
            if dictObject.get('wait').is_set():
                return dictObject.get('stopFlag')
            ltp=item.get('sellLtpFunction')() #tokenDict.get(item.get('instrument_token')).get('ltp',0)
            print(f"bid price {ltp}")
            return (ltp<=item.get('triggerPrice') and ltp!=0) or dictObject.get('stopFlag')
            # if second>2:
            #     dictObject.get('wait').wait(second-2)
            #     if dictObject.get('wait').is_set():
            #         return dictObject.get('stopFlag')
            # else:            
            #     ltp=item.get('sellLtpFunction')() #tokenDict.get(item.get('instrument_token')).get('ltp',0)
            #     print(f"bid price {ltp}")
            #     return (ltp<=item.get('triggerPrice') and ltp!=0) or dictObject.get('stopFlag')
        radheUtils.conditionStopper(sellCon,trueFunc)



def waitToGetServerResponse(processId):
    def hi():
        pass
    def condition():
        # global timeout
        # timeout=timeout-1
        return processId in processedData.keys()
    radheUtils.conditionStopper(condition,hi,1)
    # if timeout==0:
    #     print('Server Timeout.')
    #     raise Exception
    return processedData.pop(processId)

def subscribe(instrumentToken,mode='LTP'):
    request={}
    request['code']=1
    request['instrumentToken']=instrumentToken
    request['mode']=mode
    processId=send(request)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        print('Subscribed...')
        while True:
            if type(tokenDict.get(instrumentToken,{}).get('ltp')) in [int,float]:
                print("Start Receiving Data")
                break
            else:
                print(type(tokenDict.get(instrumentToken,{}).get('ltp')))
                print('Waiting to get Live Data')
                time.sleep(0.5)
                
        
        
    else:
        print("Error While Sending Subscribe request to local server")
    

def getOrderStatus(orderId,processId):
    request={}
    request['code']=4
    request['orderId']=orderId
    processId=send(request,processId)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        return data
    
def placeOrderToLocalServer(item,confirmation=1):
    request={}
    request['code']=3
    rawItem=item.copy()
    rawItem.pop('buyLtpFunction')
    rawItem.pop('sellLtpFunction')      
        
    request['data']=rawItem
    processId=send(request)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        if data.get('status')==1:
            
            if confirmation==1:
                def condition():
                    dataStatus=getOrderStatus(data.get('orderId'),processId)
                    # if dataStatus.get('status')==0:
                    #     return True
                    return dataStatus.get('orderStatus') in ['COMPLETE','REJECTED','CANCELLED']
                def hi():
                    pass
                radheUtils.conditionStopper(condition,hi,1)
            dataStatus=getOrderStatus(data.get('orderId'),processId)
            dataStatus['msg']=dataStatus.get('order').get('msg')
            dataStatus['orderId']=data.get('orderId')
            return dataStatus
        else:
            return data
    else:
        print("Error While Sending Order request to local server")
        return {'status':0,'msg':"Error While Sending Request to Local server"}



def outputThread():
    try:
        wb=xlwings.Book(excelFileLocation)
        ws=wb.sheets[excelSheetName]
        while True:
            try:
                pop=outputQueue.pop(0)
                writeOutput(ws,pop.get('excelRowId'),pop.get('data'))
            except:
                time.sleep(1)
    except Exception as e:
        print(f'Error in Output Thread {e}')
        pass
def writeOutput(ws,pnt,datas):
    for data in datas.keys():
        ws.range(f'{data}{pnt}').value=datas.get(data)

threading.Thread(target=orderDecoder).start()
threading.Thread(target=outputThread).start()
# threading.Thread(target=positions).start()

pointer=startExcelPointer
ws.range(f'{excelRef.get("command")}{pointer}:{excelOutputRef.get("orderId")}100').value=''

while True:
    try:
        # print("Server is running...")
        if ws.range(f'{excelRef.get("serial")}{pointer}').value==None:
            pointer=startExcelPointer
            time.sleep(1)
            continue
        command=radheUtils.low(ws.range(f'{excelRef.get("command")}{pointer}').value)
        if command in [0,9,'0','9','r','c','x']:
            # outputQueue.append({'excelRowId':pointer,'data':['','','']})
            rowNoQueue.append([pointer,command])
            ws.range(f'{excelRef.get("command")}{pointer}').value=f'Detected {command} on {datetime.datetime.now().strftime("%H:%M:%S")}'
        pointer+=1
    except Exception as e:
        print(e)
