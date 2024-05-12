Consdiering the increased complexity of the commands and the order in which they arrive, we need to store the commands in a queue like format and populate it in a seperate order.

Our current parser only handles commands and not responses. We need to find a way to handle this. 
Responses are usually simple strings:
+<Response>\r\n
