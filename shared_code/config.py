acceptable_ta_type = ['SA', 'RR', 'RT', 'VR', 'Sales', 'ReturnWithReceipt', 'Return', 'ManualInvoiceReturn',
                          'ManualInvoiceSale']
dap_division_list=['68','70']

postgresSqlConn = {'host':'c-dfs-aass-dp-nprd-cosmospostgres-01.4pg3oplprz3eja.postgres.cosmos.azure.com',
                   'database':'citus',
                   'user':'citus'}
americaDivision ={'11':'AMERICA', '41':'AMERICA', '47':'AMERICA'}

def add(a,b):
    return a+b