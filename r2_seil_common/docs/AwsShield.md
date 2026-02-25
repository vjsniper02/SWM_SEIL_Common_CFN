> This document mainly outlines the findings of how AWS Shield can offer to our platform

### In short, not a lot
`AWS Shield Advanced` can protect those resource types:
- Cloudfront distribution
- Route 53 Hosted Zone
- Global Accelerator
- Application load balancer
- Classic load balancer
- Elastic IP address

We don't currently expose our APIs through these services. Although, if required we can add an ALB in front of API Gateway to get the protection.

It is most useful for protecting against sophisticated DDoS attacks. We are not building a public facing platform (albeit APIs could be publicly accessible knows where it is - i.e. security through obscurity), as such AWS Shield Advanced is not an effective investment. It is like a Level 3/Level 4 firewall.

In addition, the free version that is `AWS Shield`, provides protection against common DDoS attacks.

On the other hand, `AWS WAF` WebACLs can be associated with these resources:
- API Gateway
- Application load balancer
- AWS AppSync

Protects against common Layer 7 attacks such as SQL injection, XSS attacks. It can be configured to provide certain protection against DDoS attacks via a rate limiting rule. However, it won't be as good as AWS Shield Advanced. Further more, we can utilise throttling and quota limit from API Gateway usage plan to reduce the traffic flooding the backend. We should use WAF and Shield (not necessarily the Advanced) together.