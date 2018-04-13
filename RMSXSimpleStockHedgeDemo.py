# RMSXSimpleStockHedgeDemo.py

import logging
import argparse
from datetime import datetime
from easymsx.easymsx import EasyMSX
from easymsx.notification import Notification as EasyMSXNotification
from easymkt.easymkt import EasyMKT
from rulemsx.rulemsx import RuleMSX
from rulemsx.ruleevaluator import RuleEvaluator
from rulemsx.action import Action
from rulemsx.datapointsource import DataPointSource
from rulemsx.rulecondition import RuleCondition

def parseCommandLine():
    
    parser = argparse.ArgumentParser(description="Bloomberg - RMSX Example - RMSXSimpleStockHedgeDemo")
    
    parser.add_argument('-p', '--percentage', help='The trigger threshold percentage of 30 Average Daily Volume', action='store', required=True)
    parser.add_argument('-t', '--ticker', help='The hedge ticker to use', action='store', required=True)
   
    options = parser.parse_args()
    
    return options


class RMSXSimpleStockHedgeDemo:
    
    def __init__(self, options):

        self.options = options
        self.easymsx = None
        
        print("Initialising RuleMSX...")
        self.rulemsx = RuleMSX(logging.DEBUG)
        print("RuleMSX initialised...")
        
        print("Initialising EasyMKT...")
        self.easymkt = EasyMKT()
        print("EasyMKT initialised...")

        print("Initialising EasyMSX...")
        self.easymsx = EasyMSX()
        print("EasyMSX initialised...")
        
        print("Create RuleSet...")
        self.build_rules()
        print("RuleSet ready...")

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
            dp_value = dataset.datapoints[self.datapoint_name].get_value()[:len(self.target_value)]
            #print("Evaluated StringEqualityEvaluator for DataPoint: " + self.datapoint_name + " of DataSet: " + dataset.name + " - Returning: " + str(dp_value==self.target_value))
            return dp_value!=self.target_value


    class OrderAmountThresholdEvaluator(RuleEvaluator):
        
        def __init__(self):
            
            super().add_dependent_datapoint_name("OrderAmount")
        
        def evaluate(self,dataset):
            order_amount = float(dataset.datapoints["OrderAmount"].get_value())
            trigger_threshold = float(dataset.datapoints["TriggerThreshold"].get_value())
            avg_vol = float(dataset.datapoints["20DayAvgVol"].get_value())
            
            #print("Order Amount: " + order_amount)
            #print("Trigger Threshold: " + trigger_threshold)
            #print("Average Volume: %f" %(avg_vol))

            return order_amount < (trigger_threshold * avg_vol)


    class SendNewRouteBB(Action):
        
        def __init__(self, easymsx):
            
            self.easymsx = easymsx
            self.done = False
            
            pass
        
        def execute(self,dataset):
            
            req = self.easymsx.create_request("RouteEx")
            
            ord_no = dataset.datapoints["OrderNumber"].get_value()
            req.set("EMSX_SEQUENCE", ord_no)
            req.set("EMSX_AMOUNT", dataset.datapoints["OrderAmount"].get_value())
            req.set("EMSX_BROKER", "BB")
            req.set("EMSX_HAND_INSTRUCTION", "ANY")
            req.set("EMSX_ORDER_TYPE", "MKT")
            req.set("EMSX_TICKER", dataset.datapoints["OrderTicker"].get_value())
            req.set("EMSX_TIF", "DAY")

            msg = self.easymsx.send_request(req)
            
            if msg.messageType()=="ErrorInfo":
                print("Failed to route order: " + ord_no)
                errorCode = msg.getElementAsInteger("ERROR_CODE")
                errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                print ("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode,errorMessage))
            else:
                print("Created route for order: " + ord_no)
                

    class SendNewRouteBMTB(Action):
        
        def __init__(self,easymsx):
            
            self.easymsx = easymsx
            self.done = False
            
            pass
        
        def execute(self,dataset):
            
            req = self.easymsx.create_request("RouteEx")
            
            ord_no = dataset.datapoints["OrderNumber"].get_value()

            req.set("EMSX_SEQUENCE", ord_no)
            req.set("EMSX_AMOUNT", dataset.datapoints["OrderAmount"].get_value())
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

            msg = self.easymsx.send_request(req)
            
            if msg.messageType()=="ErrorInfo":
                print("Failed to route order: " + ord_no)
                errorCode = msg.getElementAsInteger("ERROR_CODE")
                errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                print ("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode,errorMessage))
            else:
                print("Created route for order: " + ord_no)
            
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
            
            req = self.easymkt.create_request("ReferenceDataRequest")
            
            req.append("securities", ticker_source)
            req.append("fields", field)

            msg=self.easymkt.send_request(req)

            #print (msg)
            
            self.value = msg.getElement("securityData").getValue(0).getElement("fieldData").getElement(field).getValue()
        
        def get_value(self):
            
            return self.value
    
    class EMSXFieldDataPointSource(DataPointSource):

        def __init__(self, field):
            self.source = field
            self.value = self.source.value()
            self.previous_value = None
            self.source.add_notification_handler(self.process_notification)
            
        def get_value(self):
            return self.value
        
        def get_previous_value(self):
            return self.previous_value
        
        def process_notification(self, notification):
            print("process_notification of EMSXFieldDataPointSource for field: " + self.source.name() +"(" + notification.source.field("EMSX_SEQUENCE").value() + ")" )
            for fc in notification.field_changes:
                print ("    >> " + fc.field.name() + ": " + fc.old_value + " / " + fc.new_value)

            self.previous_value = self.value
            self.value = notification.field_changes[0].new_value                
            if self.previous_value != self.value:
                print("Value has changed, calling set_stale...")
                super().set_stale()
     

    class RouteFillOccured(RuleEvaluator):
        
        def __init__(self):
            print("Add route fill occured - setting dependency on RouteFilled datapoint")
            super().add_dependent_datapoint_name("RouteFilled")
        
        def evaluate(self,dataset):

            field_source = dataset.datapoints["RouteFilled"].datapoint_source
            
            # Fills only valid if status at time of fill was either working or partfill. If status was filled, then this is a history fill.
            status = dataset.datapoints["RouteStatus"].datapoint_source
            
            previous_status = status.get_previous_value()
            
            
            current_filled =  int(field_source.get_value())
            
            pf = field_source.get_previous_value()
            if not pf is None:
                previous_filled =  int(pf)
            else:
                previous_filled = 0
            
            print ("RouteFillOccured test for : " + dataset.datapoints["RouteOrderNumber"].get_value())
            
            if current_filled > previous_filled and (previous_status == "WORKING" or previous_status == "PARTFILL"):
                filled_amount = current_filled - previous_filled
                dataset.datapoints["FillAmount"].datapoint_source.set_value(filled_amount)
                print("Fill detected: " + str(filled_amount))
                return True
            else:
                print("No Fill detected (Current: %d  Previous: %d)" % (current_filled, previous_filled))
                return False
            

    class RouteExchangeUS(RuleEvaluator):
        
        def __init__(self,easymsx):
            self.easymsx = easymsx
            super().add_dependent_datapoint_name("RouteOrderNumber")
        
        def evaluate(self,dataset):

            ord_no = int(dataset.datapoints["RouteOrderNumber"].get_value())
            print("looking for order: " + str(ord_no))
            o = self.easymsx.orders.get_by_sequence_no(ord_no)
            
            exch=""
            
            if not o is None:
                print("Found order.")
                exch = o.field("EMSX_EXCHANGE").value()
            else:
                print("Failed to find order.")

            print("Evaluating fill route exchange: " + exch + "(returning : " + str(exch=="US") + ")")
            return exch=="US"
        
    class SendHedgeOrder(Action):
        
        def __init__(self, easymsx):
            
            self.easymsx = easymsx
            self.done = False
            
            pass
        
        def execute(self,dataset):
            
            ord_no = int(dataset.datapoints["RouteOrderNumber"].get_value())
            o = self.easymsx.orders.get_by_sequence_no(ord_no)

            req = self.easymsx.create_request("CreateOrderAndRouteEx")

            req.set("EMSX_TICKER", dataset.datapoints["HedgeTicker"].get_value())
            req.set("EMSX_AMOUNT", dataset.datapoints["HedgeAmount"].get_value())
            req.set("EMSX_ORDER_TYPE", "MKT")
            req.set("EMSX_TIF", "DAY")
            req.set("EMSX_HAND_INSTRUCTION", "ANY")
            if o.field("EMSX_SIDE").value() == "BUY":
                req.set("EMSX_SIDE", "SELL")
            else:
                req.set("EMSX_SIDE", "BUY")
            req.set("EMSX_BROKER", "EFIX")
            req.set("EMSX_NOTES","HEDGE:" + str(ord_no))

            msg = self.easymsx.send_request(req)
            
            if msg.messageType()=="ErrorInfo":
                print("Failed to create hedge order: " + ord_no)
                errorCode = msg.getElementAsInteger("ERROR_CODE")
                errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                print ("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode,errorMessage))
            else:
                print("Created hedge order for : " + str(ord_no))

            
    class GenericValueDataPointSource(DataPointSource):
        
        def __init__(self, initial_value):
            self.value = initial_value
            
        def get_value(self):
            return self.value
        
        def set_value(self, new_value):
            self.value = new_value
            super().set_stale()



    def build_rules(self):
        
        print("Building Rules...")

        cond_order_status_new = RuleCondition("OrderStatusIsNew", self.StringEqualityEvaluator("OrderStatus","NEW"))
        cond_order_not_hedge = RuleCondition("OrderNotHedge", self.StringInequalityEvaluator("OrderNotes","HEDGE"))
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


        cond_route_fill_occured = RuleCondition("RouteFillOccured", self.RouteFillOccured())
        cond_route_exchange_US = RuleCondition("RouteExchangeUS", self.RouteExchangeUS(self.easymsx))
        cond_route_not_hedge = RuleCondition("RouteNotHedge", self.StringInequalityEvaluator("RouteNotes","HEDGE"))

        action_send_hedge_order = self.rulemsx.create_action("SendHedgeOrder", self.SendHedgeOrder(self.easymsx))
        
        demo_route_ruleset = self.rulemsx.create_ruleset("demoRouteRuleSet")
        
        rule_hedge_order_US = demo_route_ruleset.add_rule("HedgeOrderUS")
        rule_hedge_order_US.add_rule_condition(cond_route_fill_occured)
        rule_hedge_order_US.add_rule_condition(cond_route_exchange_US)
        rule_hedge_order_US.add_rule_condition(cond_route_not_hedge)
        rule_hedge_order_US.add_action(action_send_hedge_order)

        print("Rules built.")


    def process_notification(self,notification):

        if notification.category == EasyMSXNotification.NotificationCategory.ORDER:
            if notification.type == EasyMSXNotification.NotificationType.NEW or notification.type == EasyMSXNotification.NotificationType.INITIALPAINT: 
                print("EasyMSX Notification ORDER -> NEW/INIT_PAINT: " + notification.source.field("EMSX_SEQUENCE").value())
                self.parse_order(notification.source)
        
        if notification.category == EasyMSXNotification.NotificationCategory.ROUTE:
            if notification.type == EasyMSXNotification.NotificationType.NEW or notification.type == EasyMSXNotification.NotificationType.INITIALPAINT: 
                print("EasyMSX Notification ROUTE -> NEW/INIT_PAINT: " + notification.source.field("EMSX_SEQUENCE").value() + "/" + notification.source.field("EMSX_ROUTE_ID").value())
                self.parse_route(notification.source)
            
        
    def parse_order(self,o):
        
        print("Parse Order: " + o.field("EMSX_SEQUENCE").value())

        new_dataset = self.rulemsx.create_dataset("DS_OR_" + o.field("EMSX_SEQUENCE").value())

        new_dataset.add_datapoint("OrderStatus", self.EMSXFieldDataPointSource(o.field("EMSX_STATUS")))
        new_dataset.add_datapoint("OrderTicker", self.EMSXFieldDataPointSource(o.field("EMSX_TICKER")))
        new_dataset.add_datapoint("OrderNumber", self.EMSXFieldDataPointSource(o.field("EMSX_SEQUENCE")))
        new_dataset.add_datapoint("OrderAmount", self.EMSXFieldDataPointSource(o.field("EMSX_AMOUNT")))
        new_dataset.add_datapoint("OrderNotes", self.EMSXFieldDataPointSource(o.field("EMSX_NOTES")))
        new_dataset.add_datapoint("TriggerThreshold", self.ConstDataPointSource(self.options.percentage))
        new_dataset.add_datapoint("20DayAvgVol", self.GetRefDataField(self.easymkt, o.field("EMSX_TICKER").value(),"VOLUME_AVG_20D"))
        new_dataset.add_datapoint("Exchange", self.GetRefDataField(self.easymkt, o.field("EMSX_TICKER").value(),"EXCH_CODE"))

        self.rulemsx.rulesets["demoOrderRuleSet"].execute(new_dataset)


    def parse_route(self,r):
        
        print("Parse Route: " + r.field("EMSX_SEQUENCE").value() + "." + r.field("EMSX_ROUTE_ID").value())
        
        new_dataset = self.rulemsx.create_dataset("DS_RT_" + r.field("EMSX_SEQUENCE").value() + "." + r.field("EMSX_ROUTE_ID").value())
    
        new_dataset.add_datapoint("RouteStatus", self.EMSXFieldDataPointSource(r.field("EMSX_STATUS")))
        new_dataset.add_datapoint("RouteOrderNumber", self.EMSXFieldDataPointSource(r.field("EMSX_SEQUENCE")))
        new_dataset.add_datapoint("RouteID", self.EMSXFieldDataPointSource(r.field("EMSX_ROUTE_ID")))
        new_dataset.add_datapoint("RouteFilled", self.EMSXFieldDataPointSource(r.field("EMSX_FILLED")))
        new_dataset.add_datapoint("RouteAmount", self.EMSXFieldDataPointSource(r.field("EMSX_AMOUNT")))
        new_dataset.add_datapoint("RouteLastShares", self.EMSXFieldDataPointSource(r.field("EMSX_LAST_SHARES")))
        new_dataset.add_datapoint("HedgeTicker", self.ConstDataPointSource(self.options.ticker))
        new_dataset.add_datapoint("FillAmount", self.GenericValueDataPointSource(0))
        new_dataset.add_datapoint("HedgeAmount", self.GenericValueDataPointSource(1))
        new_dataset.add_datapoint("RouteNotes", self.EMSXFieldDataPointSource(r.field("EMSX_NOTES")))
        
        self.rulemsx.rulesets["demoRouteRuleSet"].execute(new_dataset)
    

    def log(msg):
    
        mytime= datetime.now()
        s  = mytime.strftime("%Y%m%d%H%M%S%f")
        print(s + "(RMSXSimpleStockHedgeDemo): \t" + msg)


if __name__ == '__main__':
    
    options=parseCommandLine()

    RMSXSimpleStockHedgeDemo = RMSXSimpleStockHedgeDemo(options);
    
    input("Press any to terminate\n")

    print("Terminating...\n")

    RMSXSimpleStockHedgeDemo.rulemsx.stop()
    
    quit()
    