 Assembly Transfer Orders
    definition: Transfer Order where Transfer Order Type = Assembly

Currently, FSA is considering Assembly Transfer Orders supply as long as Commited != 0

The issue with this is that once the Transfer Order is moved to the Pending Receipt status, Commitment = 0 even though this status still has inventory heading to the location as Supply

Lifecycle and inventory impact of Transfer Order:

* Pending Approval
    Inventory is not Committed
* Pending Fulfillment
    Transfer Order Request may or may not be created
    Inventory is Committed
* Pending Receipt
    TOR is created 
    Item Fulfillment is created
    Inventory is In Transit on the item record
    Inventory is no longer Committed
* Received
    Inventory is reflected on the received locations on hand value and is no longer committed or in transit


Acceptance Criteria:

* The business expects FSA to interpret the following header level Transfer Order Statuses as Supply and Demand:
    Pending Approval - Demand
    Pending Fulfillment - Supply
    Pending Receipt - Supply
    Received - Inventory enters On Hand
        FSA *should no longer* be including the Transfer Order as Supply or Demand

Closed or Rejected Status Transfer Orders are not considered supply or demand


PO indicators												Reason for date assignment
==============												==========================
PO_INDICATOR = 1, where LOCATION is assigned				Supply available, location already assigned
PO_INDICATOR = 1, no LOCATION assigned						Supply available from inventory
PO_INDICATOR = 0											Supply available from inventory, none will remain
PO_INDICATOR = -1, PO_INDICATOR_ASSIGN = 1					Backorder with pending purchase order
PO_INDICATOR = -1, PO_INDICATOR_ASSIGN = 0					Backorder with no pending purchase order

https://3919805.app.netsuite.com/app/accounting/transactions/trnfrord.nl?id=17946291&whence=
https://amplify-education.atlassian.net/browse/SCCS-608 == ticket for policy change
