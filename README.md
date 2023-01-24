# ds_assignment_1

## Design choices
* Start server design with Flask for http API
    * chosen flask (supports multi-threading and multi-processing)
* for Part A, 
    * **processors are limited to 1:** because queue data structures are maintained in-memory and needed to be preserved between requests in a flask cintext.
    * **threading is enabled:** for performance and acheiving *distributed-queue* functuinality. 
    