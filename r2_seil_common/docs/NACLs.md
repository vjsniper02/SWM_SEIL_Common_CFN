#NACL's
- The public inbound rules dont allow 80 or 443 in from the internet. If a service is used that requires this then it needs to be added to the public NACL. API gateways and appflow's do not require inbound NACL rules.
- The below NACL's will be edited when we are provided with the on prem SWM IP ranges. 
- The SSH rule will be edited to allow the on prem range if a SFTP endpoint is used to route SFTP traffic over the direct connect link


# Prod NACLs
| PUBLIC           |             |          |            |              |              |                                                                    |
|------------------|-------------|----------|------------|--------------|--------------|--------------------------------------------------------------------|
| Inbound traffic  |             |          |            |              |              |                                                                    |
|      Rule #      |     Type    | Protocol | Port Range |    Source    | Allow / Deny |                              Comments                              |
|       10080      |  HTTP (80)  |  TCP (6) |     80     | 10.80.0.0/20 |     ALLOW    | Allow   Cross AZ PUBLIC_IN ALL traffic                             |
|       10443      | HTTPS (443) |  TCP (6) |     443    | 10.80.0.0/20 |     ALLOW    | Allow   Customer inbound on HTTP                                   |
|       10443      |     SSH     |  TCP (6) |     22     |   0.0.0.0/0  |     ALLOW    | SSH   traffic in. Will change to on prem range if using endpoints  |
|       20000      |  Custom TCP |  TCP (6) | 1024-65535 |   0.0.0.0/0  |     ALLOW    | Allow   Ephemeral reply from internal Web instance                 |
|         *        | ALL Traffic |    ALL   |     ALL    |   0.0.0.0/0  |     DENY     | Denied   anything else (default)                                   |
| Outbound traffic |             |          |            |              |              |                                                                    |
|      Rule #      |     Type    | Protocol | Port Range |  Destination | Allow / Deny |                              Comments                              |
|       10000      |  Custom TCP |  TCP (6) | 1024-65535 | 10.80.0.0/20 |     ALLOW    | Allow   reply from the NAT gateway to the AWS IP Ephemeral traffic |
|       10080      |  HTTP (80)  |  TCP (6) |     80     |   0.0.0.0/0  |     ALLOW    | Allow   HTTP out from the NAT Gateway to the Internet              |
|       10443      | HTTPS (443) |  TCP (6) |     443    |   0.0.0.0/0  |     ALLOW    | Allow   HTTPS out from the NAT Gateway to the Internet             |
|                  |             |          |            |              |              |                                                                    |
|         *        | ALL Traffic |    ALL   |     ALL    |   0.0.0.0/0  |     DENY     | Denied   anything else (default)                                   |
|                  |             |          |            |              |              |                                                                    |
| PRIVATE          |             |          |            |              |              |                                                                    |
| Inbound traffic  |             |          |            |              |              |                                                                    |
|      Rule #      |     Type    | Protocol | Port Range |    Source    | Allow / Deny |                              Comments                              |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.0.0/20 |     ALLOW    | Allows   ALL traffic ALL Ports AWS range                           |
|       10010      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10020      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10030      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       20000      |  Custom TCP |  TCP (6) | 1024-65535 |   0.0.0.0/0  |     ALLOW    | Allow   Ephemeral reply for web surfing                            |
|         *        | ALL Traffic |    ALL   |     ALL    |   0.0.0.0/0  |     DENY     | Denied   anything else (default)                                   |
| Outbound traffic |             |          |            |              |              |                                                                    |
|      Rule #      |     Type    | Protocol | Port Range |  Destination | Allow / Deny |                              Comments                              |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.0.0/20 |     ALLOW    | Allows   ALL traffic ALL Ports AWS range                           |
|       10010      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10020      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10030      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10080      |  HTTP (80)  |  TCP (6) |     80     |   0.0.0.0/0  |     ALLOW    | Allow   HTTP out from the instance to NAT gateway in SS            |
|       10443      | HTTPS (443) |  TCP (6) |     443    |   0.0.0.0/0  |     ALLOW    | Allow   HTTPS out from the instance to NAT gateway in SS           |
|         *        | ALL Traffic |    ALL   |     ALL    |   0.0.0.0/0  |     DENY     | Denied   anything else (default)                                   |
|                  |             |          |            |              |              |                                                                    |
| Restricted       |             |          |            |              |              |                                                                    |
| Inbound traffic  |             |          |            |              |              |                                                                    |
|      Rule #      |     Type    | Protocol | Port Range |    Source    | Allow / Deny |                              Comments                              |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.8.0/22 |     ALLOW    | Allow   Cross AZ ALL traffic ALL Ports                             |
|       10010      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10020      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10030      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10050      | ALL Traffic |    ALL   |     ALL    | 10.80.0.0/21 |     ALLOW    | Allows   SS private range ALL Ports                                |
|                  |             |          |            |              |              |                                                                    |
|         *        | ALL Traffic |    ALL   |     ALL    |   0.0.0.0/0  |     DENY     | Denied   anything else (default)                                   |
| Outbound traffic |             |          |            |              |              |                                                                    |
|      Rule #      |     Type    | Protocol | Port Range |  Destination | Allow / Deny |                              Comments                              |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.8.0/22 |     ALLOW    | Allow   Cross AZ ALL traffic ALL Ports                             |
|       10010      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10020      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10030      | ALL Traffic |    ALL   |     ALL    |    x.x.x.x   |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                       |
|       10050      | ALL Traffic |    ALL   |     ALL    | 10.80.0.0/21 |     ALLOW    | Allows   SS private range ALL Ports                                |
|                  |             |          |            |              |              |                                                                    |
|         *        | ALL Traffic |    ALL   |     ALL    |   0.0.0.0/0  |     DENY     | Denied   anything else (default)                                   |


# Non Prod NACLs
| PUBLIC           |             |          |            |                |              |                                                                        |
|------------------|-------------|----------|------------|----------------|--------------|------------------------------------------------------------------------|
| Inbound traffic  |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |     Source     | Allow / Deny |                                Comments                                |
|       10080      |  HTTP (80)  |  TCP (6) |     80     | 10.80.128.0/20 |     ALLOW    | Allow   HTTP from AWS IP range                                         |
|       10443      | HTTPS (443) |  TCP (6) |     443    | 10.80.128.0/20 |     ALLOW    | Allow   HTTPS from AWS IP range                                        |
|       15000      |     SSH     |  TCP (6) |     22     |    0.0.0.0/0   |     ALLOW    | SSH   traffic in. Will change to on prem range if using endpoints      |
|       20000      |  Custom TCP |  TCP (6) | 1024-65535 |    0.0.0.0/0   |     ALLOW    | Allow   replies from external website to NAT Gateway Ephemeral traffic |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
| Outbound traffic |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |   Destination  | Allow / Deny |                                Comments                                |
|       10000      |  Custom TCP |  TCP (6) | 1024-65535 | 10.80.128.0/20 |     ALLOW    | Allow   reply from the NAT gateway to the AWS IP Ephemeral traffic     |
|       10080      |  HTTP (80)  |  TCP (6) |     80     |    0.0.0.0/0   |     ALLOW    | Allow   HTTP out from the NAT Gateway to the Internet                  |
|       10443      | HTTPS (443) |  TCP (6) |     443    |    0.0.0.0/0   |     ALLOW    | Allow   HTTPS out from the NAT Gateway to the Internet                 |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
|                  |             |          |            |                |              |                                                                        |
| PRIVATE          |             |          |            |                |              |                                                                        |
| Inbound traffic  |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |     Source     | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.128.0/20 |     ALLOW    | Allows   ALL traffic ALL Ports AWS range                               |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       20000      |  Custom TCP |  TCP (6) | 1024-65535 |    0.0.0.0/0   |     ALLOW    | Allow   Ephemeral reply for web surfing                                |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
| Outbound traffic |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |   Destination  | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.128.0/20 |     ALLOW    | Allows   ALL traffic ALL Ports AWS range                               |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10080      |  HTTP (80)  |  TCP (6) |     80     |    0.0.0.0/0   |     ALLOW    | Allow   HTTP out from the instance to NAT gateway in SS                |
|       10443      | HTTPS (443) |  TCP (6) |     443    |    0.0.0.0/0   |     ALLOW    | Allow   HTTPS out from the instance to NAT gateway in SS               |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
|                  |             |          |            |                |              |                                                                        |
| DATABASES        |             |          |            |                |              |                                                                        |
| Inbound traffic  |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |     Source     | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.136.0/22 |     ALLOW    | Allow   Cross AZ ALL traffic ALL Ports                                 |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10100      | ALL Traffic |    ALL   |     ALL    | 10.80.128.0/21 |     ALLOW    | Allows   ALL traffic ALL Ports Own VPC Private Range                   |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
| Outbound traffic |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |   Destination  | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.136.0/22 |     ALLOW    | Allow   Cross AZ ALL traffic ALL Ports                                 |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10100      | ALL Traffic |    ALL   |     ALL    | 10.80.128.0/21 |     ALLOW    | Allows   ALL traffic ALL Ports Own VPC Private Range                   |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |

# DEV NACLs
| PUBLIC           |             |          |            |                |              |                                                                        |
|------------------|-------------|----------|------------|----------------|--------------|------------------------------------------------------------------------|
| Inbound traffic  |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |     Source     | Allow / Deny |                                Comments                                |
|       10080      |  HTTP (80)  |  TCP (6) |     80     | 10.80.144.0/20 |     ALLOW    | Allow   HTTP from AWS IP range                                         |
|       10443      | HTTPS (443) |  TCP (6) |     443    | 10.80.144.0/20 |     ALLOW    | Allow   HTTPS from AWS IP range                                        |
|       15000      |     SSH     |  TCP (6) |     22     |    0.0.0.0/0   |     ALLOW    | SSH   traffic in. Will change to on prem range if using endpoints      |
|       20000      |  Custom TCP |  TCP (6) | 1024-65535 |    0.0.0.0/0   |     ALLOW    | Allow   replies from external website to NAT Gateway Ephemeral traffic |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
| Outbound traffic |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |   Destination  | Allow / Deny |                                Comments                                |
|       10000      |  Custom TCP |  TCP (6) | 1024-65535 | 10.80.144.0/20 |     ALLOW    | Allow   reply from the NAT gateway to the AWS IP Ephemeral traffic     |
|       10080      |  HTTP (80)  |  TCP (6) |     80     |    0.0.0.0/0   |     ALLOW    | Allow   HTTP out from the NAT Gateway to the Internet                  |
|       10443      | HTTPS (443) |  TCP (6) |     443    |    0.0.0.0/0   |     ALLOW    | Allow   HTTPS out from the NAT Gateway to the Internet                 |
|                  |             |          |            |                |              |                                                                        |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
|                  |             |          |            |                |              |                                                                        |
| PRIVATE          |             |          |            |                |              |                                                                        |
| Inbound traffic  |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |     Source     | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.144.0/20 |     ALLOW    | Allows   ALL traffic ALL Ports AWS range                               |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       20000      |  Custom TCP |  TCP (6) | 1024-65535 |    0.0.0.0/0   |     ALLOW    | Allow   Ephemeral reply for web surfing                                |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
| Outbound traffic |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |   Destination  | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.144.0/20 |     ALLOW    | Allows   ALL traffic ALL Ports AWS range                               |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10080      |  HTTP (80)  |  TCP (6) |     80     |    0.0.0.0/0   |     ALLOW    | Allow   HTTP out from the instance to NAT gateway in SS                |
|       10443      | HTTPS (443) |  TCP (6) |     443    |    0.0.0.0/0   |     ALLOW    | Allow   HTTPS out from the instance to NAT gateway in SS               |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
|                  |             |          |            |                |              |                                                                        |
| DATABASES        |             |          |            |                |              |                                                                        |
| Inbound traffic  |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |     Source     | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.152.0/22 |     ALLOW    | Allow   Cross AZ ALL traffic ALL Ports                                 |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10050      | ALL Traffic |    ALL   |     ALL    | 10.80.144.0/21 |     ALLOW    | Allows   ALL traffic ALL Ports Own VPC Private Range                   |
|                  |             |          |            |                |              |                                                                        |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |
| Outbound traffic |             |          |            |                |              |                                                                        |
|      Rule #      |     Type    | Protocol | Port Range |   Destination  | Allow / Deny |                                Comments                                |
|       10000      | ALL Traffic |    ALL   |     ALL    | 10.80.152.0/22 |     ALLOW    | Allow   Cross AZ ALL traffic ALL Ports                                 |
|       10010      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10020      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10030      | ALL Traffic |    ALL   |     ALL    |     x.x.x.x    |     ALLOW    | Allows   ALL traffic ALL Ports On-Prem range                           |
|       10050      | ALL Traffic |    ALL   |     ALL    | 10.80.144.0/21 |     ALLOW    | Allows   ALL traffic ALL Ports Own VPC Private Range                   |
|                  |             |          |            |                |              |                                                                        |
|         *        | ALL Traffic |    ALL   |     ALL    |    0.0.0.0/0   |     DENY     | Denied   anything else (default)                                       |