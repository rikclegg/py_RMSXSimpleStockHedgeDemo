# RMSXSimpleStockHedgeDemo.py

import logging
import argparse

from easymsx.easymsx import EasyMSX
from easymsx.notification import Notification as EasyMSXNotification
from easymkt.easymkt import EasyMKT
from easymkt.notification import Notification as EasyMKTNotification
from rulemsx.rulemsx import RuleMSX
from rulemsx.ruleevaluator import RuleEvaluator
from rulemsx.action import Action
from rulemsx.datapointsource import DataPointSource
from rulemsx.rulecondition import RuleCondition

def parseCommandLine():
    
    parser = argparse.ArgumentParser(description="Bloomberg - RMSX Example - RMSXSimpleStockHedgeDemo")
    
    parser.add_argument('-t', '--threshold', help='The trigger threshold percentage of 30 Average Daily Volume', action='store', required=True)
    parser.add_argument('-h', '--hedge', help='The hedge ticker to use', action='store', required=True)
   
    options = parser.parse_args()
    
    return options


class RMSXSimpleStockHedgeDemo:
    
    def __init__(self, options):

        self.options = options
        
        print("Initialising RuleMSX...")
        self.rulemsx = RuleMSX(logging.CRITICAL)
        print("RuleMSX initialised...")
        
        print("Create RuleSet...")
        self.build_rules()
        print("RuleSet ready...")
        
        print("Initialising EasyMKT...")
        self.easymkt = EasyMKT()
        print("EasyMKT initialised...")

        print("Initialising EasyMSX...")
        self.easymsx = EasyMSX()
        print("EasyMSX initialised...")
        
        self.easymsx.orders.add_notification_handler(self.process_notification)
        self.easymsx.routes.add_notification_handler(self.process_notification)

        self.easymsx.start()
             
    class StringEqualityEvaluator(RuleEvaluator):
        
        def __init__(self, datapoint_name, target_value, additional_dep=None):
            
            #print("Initializing StringEqualityEvaluator for DataPoint: " + datapoint_name)

            self.datapoint_name = datapoint_name
            self.target_value = target_value
            super().add_dependent_datapoint_name(datapoint_name)
            if not additional_dep==None:
                super().add_dependent_datapoint_name(additional_dep)

            #print("Initialized StringEqualityEvaluator for DataPoint: " + datapoint_name)
        
        def evaluate(self,dataset):
            dp_value = dataset.datapoints[self.datapoint_name].get_value()
            #print("Evaluated StringEqualityEvaluator for DataPoint: " + self.datapoint_name + " of DataSet: " + dataset.name + " - Returning: " + str(dp_value==self.target_value))
            return dp_value==self.target_value
        
    class StringInequalityEvaluator(RuleEvaluator):
        
        def __init__(self, datapoint_name, target_value, additional_dep=None):
            
            #print("Initializing StringEqualityEvaluator for DataPoint: " + datapoint_name)

            self.datapoint_name = datapoint_name
            self.target_value = target_value
            super().add_dependent_datapoint_name(datapoint_name)
            if not additional_dep==None:
                super().add_dependent_datapoint_name(additional_dep)

            #print("Initialized StringEqualityEvaluator for DataPoint: " + datapoint_name)
        
        def evaluate(self,dataset):
            dp_value = dataset.datapoints[self.datapoint_name].get_value()
            #print("Evaluated StringEqualityEvaluator for DataPoint: " + self.datapoint_name + " of DataSet: " + dataset.name + " - Returning: " + str(dp_value==self.target_value))
            return dp_value!=self.target_value


    class OrderAmountThresholdEvaluator(RuleEvaluator):
        
        def __init__(self):
            
            super().add_dependent_datapoint_name("OrderAmount")
        
        def evaluate(self,dataset):
            order_amount = dataset.datapoints["OrderAmount"].get_value()
            trigger_threshold = dataset.datapoints["TriggerThreshold"].get_value()
            avg_vol = dataset.datapoints["20DayAvgVol"].get_value()
            
            return order_amount < (trigger_threshold * avg_vol)


    class SendNewRouteBB(Action):
        
        def __init__(self, easymsx):
            
            self.easymsx = easymsx
            self.done = False
            
            pass
        
        def execute(self,dataset):
            
            req = self.easymsx.emsx_service.createRequest("RouteEx")
            
            req.set("EMSX_SEQUENCE", dataset.datapoints["OrderNumber"].get_value())
            req.set("EMSX_AMOUNT", dataset.datapoints["OrderNumber"].get_value())
            req.set("EMSX_BROKER", "BB")
            req.set("EMSX_HAND_INSTRUCTION", "ANY")
            req.set("EMSX_ORDER_TYPE", "MKT")
            req.set("EMSX_TICKER", dataset.datapoints["OrderTicker"].get_value())
            req.set("EMSX_TIF", "DAY")

            self.easymsx.submit_request(req, self.processResponse)
            
            #wait for response
            while(not self.done):
                pass
            
        def processResponse(self, msg):
        
            if msg.messageType() == "ErrorInfo":
                errorCode = msg.getElementAsInteger("ERROR_CODE")
                errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                print("Route request error >> ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode,errorMessage))

            self.done=True


    class SendNewRouteBMTB(Action):
        
        def __init__(self,easymsx):
            
            self.easymsx = easymsx
            self.done = False
            
            pass
        
        def execute(self,dataset):
            
            req = self.easymsx.emsx_service.createRequest("RouteEx")
            
            req.set("EMSX_SEQUENCE", dataset.datapoints["OrderNumber"].get_value())
            req.set("EMSX_AMOUNT", dataset.datapoints["OrderNumber"].get_value())
            req.set("EMSX_BROKER", "BMTB")
            req.set("EMSX_HAND_INSTRUCTION", "ANY")
            req.set("EMSX_ORDER_TYPE", "MKT")
            req.set("EMSX_TICKER", dataset.datapoints["OrderTicker"].get_value())
            req.set("EMSX_TIF", "DAY")

            strategy = req.getElement("EMSX_STRATEGY_PARAMS")
            strategy.setElement("EMSX_STRATEGY_NAME", "VWAP")
                
            indicator = strategy.getElement("EMSX_STRATEGY_FIELD_INDICATORS")
            data = strategy.getElement("EMSX_STRATEGY_FIELDS")
                
            data.appendElement().setElement("EMSX_FIELD_DATA", "09:30:00")  # StartTime
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 0)

            data.appendElement().setElement("EMSX_FIELD_DATA", "10:30:00")   # EndTime
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 0)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # Max%Volume
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
               
            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # %AMSession
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # OPG
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # MOC
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # CompletePX
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
               
            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # TriggerPX
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # DarkComplete
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # DarkCompPX
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # RefIndex
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            data.appendElement().setElement("EMSX_FIELD_DATA", "")           # Discretion
            indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

            self.easymsx.submit_request(req, self.processResponse)
            
            #wait for response
            while(not self.done):
                pass
            
        def processResponse(self, msg):
        
            if msg.messageType() == "ErrorInfo":
                errorCode = msg.getElementAsInteger("ERROR_CODE")
                errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                print("Route request error >> ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode,errorMessage))

            self.done=True


    class EMSXFieldDataPointSource(DataPointSource):

        def __init__(self, field):
            #print("Initializing EMSXFieldDataPointSource for field: " + field.name())
            self.source = field
            self.prev = None
            field.add_notification_handler(self.process_notification)
            
        def get_value(self):
            #print("GetValue of EMSXFieldDataPointSource for field: " + self.source.name())
            return self.source.value()
        
        def get_prev(self):
            return self.prev
        
        def process_notification(self, notification):
            #print("SetValue of EMSXFieldDataPointSource for field: " + self.source.name())
            self.prev.set_value(notification.field_changes[0].old_value)
            super().set_stale()
    
    class ConstDataPointSource(DataPointSource):
        
        def __init__(self, value):
            self.value=value
            
        def get_value(self):
            return self.value
        
    class GetRefDataField(DataPointSource):
        
        def __init__(self, easymkt, ticker_source, field):

            self.easymkt = easymkt
            self.ticker_source = ticker_source
            self.field = field
            
            req = self.easymkt.emkt_service.createRequest("RouteEx")
            
            req.set("EMSX_SEQUENCE", dataset.datapoints["OrderNumber"].get_value())
            req.set("EMSX_AMOUNT", dataset.datapoints["OrderNumber"].get_value())
            req.set("EMSX_BROKER", "BMTB")
            req.set("EMSX_HAND_INSTRUCTION", "ANY")
            req.set("EMSX_ORDER_TYPE", "MKT")
            req.set("EMSX_TICKER", dataset.datapoints["OrderTicker"].get_value())
            req.set("EMSX_TIF", "DAY")

        def processResponse(self, msg):
        
            if msg.messageType() == "ErrorInfo":
                errorCode = msg.getElementAsInteger("ERROR_CODE")
                errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                print("Route request error >> ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode,errorMessage))

            self.done=True

        def get_value(self):
            
            return self.value
    
            
            
    def build_rules(self):
        
        print("Building Rules...")

        cond_order_status_new = RuleCondition("OrderStatusIsNew", self.StringEqualityEvaluator("OrderStatus","NEW"))
        cond_order_not_hedge = RuleCondition("OrderNotHedge", self.StringInequalityEvaluator("Notes","HEDGE"))
        cond_order_amount_trigger = RuleCondition("OrderAmountTrigger", self.OrderAmountThresholdEvaluator())
        cond_order_exchange_US = RuleCondition("OrderExchangeUS", self.StringEqualityEvaluator("Exchange","US"))
        cond_order_exchange_LN = RuleCondition("OrderExchangeLN", self.StringEqualityEvaluator("Exchange","LN"))

        action_order_send_new_route_BB = self.rulemsx.create_action("OrderSendNewRouteBB", self.SendNewRouteBB(self.easymsx))
        action_order_send_new_route_BMTB = self.rulemsx.create_action("OrderSendNewRouteBMTB", self.SendNewRouteBMTB(self.easymsx))

        demo_order_ruleset = self.rulemsx.create_ruleset("demoOrderRuleSet")
        
        rule_new_order_US = demo_order_ruleset.add_rule("NewOrderUS")
        rule_new_order_US.add_rule_condition(cond_order_status_new)
        rule_new_order_US.add_rule_condition(cond_order_not_hedge)
        rule_new_order_US.add_rule_condition(cond_order_amount_trigger)
        rule_new_order_US.add_rule_condition(cond_order_exchange_US)
        rule_new_order_US.add_action(action_order_send_new_route_BB)
        
        rule_new_order_LN = demo_order_ruleset.add_rule("NewOrderLN")
        rule_new_order_LN.add_rule_condition(cond_order_status_new)
        rule_new_order_LN.add_rule_condition(cond_order_not_hedge)
        rule_new_order_LN.add_rule_condition(cond_order_amount_trigger)
        rule_new_order_LN.add_rule_condition(cond_order_exchange_LN)
        rule_new_order_LN.add_action(action_order_send_new_route_BMTB)

        print("Rules built.")


    def process_notification(self,notification):

        if notification.category == EasyMSXNotification.NotificationCategory.ORDER:
            if notification.type == EasyMSXNotification.NotificationType.NEW or notification.type == EasyMSXNotification.NotificationType.INITIALPAINT: 
                print("EasyMSX Notification ORDER -> NEW/INIT_PAINT: " + notification.source.field("EMSX_SEQUENCE").value())
                self.parse_order(notification.source)
        
        #if notification.category == EasyMSXNotification.NotificationCategory.ROUTE:
        #    if notification.type == EasyMSXNotification.NotificationType.NEW or notification.type == EasyMSXNotification.NotificationType.INITIALPAINT: 
        #        print("EasyMSX Notification ROUTE -> NEW/INIT_PAINT: " + notification.source.field("EMSX_SEQUENCE").value() + "/" + notification.source.field("EMSX_ROUTE_ID").value())
        #        self.parse_route(notification.source)
            
        
    def parse_order(self,o):
        
        print("Parse Order: " + o.field("EMSX_SEQUENCE").value())

        new_dataset = self.rulemsx.create_dataset("DS_OR_" + o.field("EMSX_SEQUENCE").value())

        new_dataset.add_datapoint("OrderStatus", self.EMSXFieldDataPointSource(o.field("EMSX_STATUS")))
        new_dataset.add_datapoint("OrderTicker", self.EMSXFieldDataPointSource(o.field("EMSX_TICKER")))
        new_dataset.add_datapoint("OrderNumber", self.EMSXFieldDataPointSource(o.field("EMSX_SEQUENCE")))
        new_dataset.add_datapoint("OrderAmount", self.EMSXFieldDataPointSource(o.field("EMSX_AMOUNT")))
        new_dataset.add_datapoint("OrderNotes", self.EMSXFieldDataPointSource(o.field("EMSX_NOTES")))
        new_dataset.add_datapoint("TriggerThreshold", self.ConstDataPointSource(self.options.threshold))
        new_dataset.add_datapoint("HedgeTicker", self.ConstDataPointSource(self.options.hedge))
        new_dataset.add_datapoint("20DayAvgVol", self.GetRefDataField(self.easymkt, "OrderTicker","VOLUME_AVG_20D"))
        new_dataset.add_datapoint("Exchange", self.GetRefDataField(self.easymkt, "OrderTicker","EXCH_CODE"))

        self.rulemsx.rulesets["demoOrderRuleSet"].execute(new_dataset)

        #print("Parse Order: " + o.field("EMSX_SEQUENCE").value()+ "...done.")

    #def parse_route(self,r):
    #    
    #    print("Parse Route: " + r.field("EMSX_SEQUENCE").value() + "/" + r.field("EMSX_ROUTE_ID").value())
    #
    #    new_dataset = self.rulemsx.create_dataset("DS_RT_" + r.field("EMSX_SEQUENCE").value() + r.field("EMSX_ROUTE_ID").value())
    #
    #    new_dataset.add_datapoint("RouteStatus", self.EMSXFieldDataPointSource(r.field("EMSX_STATUS")))
    #    new_dataset.add_datapoint("OrderNumber", self.EMSXFieldDataPointSource(r.field("EMSX_SEQUENCE")))
    #    new_dataset.add_datapoint("RouteID", self.EMSXFieldDataPointSource(r.field("EMSX_ROUTE_ID")))
    #    new_dataset.add_datapoint("Filled", self.EMSXFieldDataPointSource(r.field("EMSX_FILLED")))
    #    new_dataset.add_datapoint("PrevFilled", self.GenericIntegerDataPointSource(0))
    #    new_dataset.add_datapoint("Amount", self.EMSXFieldDataPointSource(r.field("EMSX_AMOUNT")))
    #
    #    self.rulemsx.rulesets["demoRouteRuleSet"].execute(new_dataset)
    #
    #    #print("Parse Route: " + r.field("EMSX_SEQUENCE").value() + r.field("EMSX_ROUTE_ID").value() + "...done.")



if __name__ == '__main__':
    
    options=parseCommandLine()

    RMSXSimpleStockHedgeDemo = RMSXSimpleStockHedgeDemo(options);
    
    input("Press any to terminate\n")

    print("Terminating...\n")

    RMSXSimpleStockHedgeDemo.rulemsx.stop()
    
    quit()
    