# Key Policy Guidelines

A KMS key alias should be used in IAM policies to lockdown the IAM role to only have access to the required keys. Alias's can be added to key policies in cloudformation templates or manually in the console.

Key policies should be created with the user root to have full permissions. This will allow us to control access to the keys through IAM policies and key alias.

We will use conditions in the IAM policy to require the KMS key to have a certain alias attached to it. Alias can only be attached to one key and the alias name cannot be used again on another KMS key within the same account.


## Examples of Key Policy, IAM Policy, Cloudformation Key and Alias Creation

### Key Policy:

> Key policy format.
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${accoundID}:root"
            },
            "Action": "kms:*",
            "Resource": "*"
        }
    ]
}
```

### IAM Policies:

> Below is an example of a IAM role with access only to the key with the alias "alias/keyaliasexample". (Permissions could be reduced)
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "kms:*",
            "Resource": "arn:aws:kms:ap-southeast-2:${accoundID}:key/*",
            "Condition": {
                "ForAnyValue:StringEquals": {
                    "kms:ResourceAliases": "alias/keyaliasexample"
                }
            }
        }
    ]
}
```

### Cloudformation Key and Alias Creation:

> Below is an example of creating a key with a key policy and then creating a alias and attaching it to the key.
```yaml
  # KMS Key
  testkmskey:
    Type: AWS::KMS::Key
    Properties:
      EnableKeyRotation: true
      KeyPolicy:
        Version: '2012-10-17'
        Statement:
        - Effect: Allow
          Principal:
            AWS: 'arn:aws:iam::${accoundID}:root'
          Action: 'kms:*'
          Resource: '*'

  # KMS Alias
  testkmsKeyAlias:
    Type: AWS::KMS::Alias
    Properties:
      AliasName: alias/keyaliasexample
      TargetKeyId: !Ref testkmskey
```