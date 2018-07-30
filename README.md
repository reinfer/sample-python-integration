Sample Python re:infer Integration
===

Generally adding verbatims to re:infer is as simple as:

```bash
curl -XPOST 'https://reinfer.io/api/voc/datasets/<owner>/<dataset>/sync' \
         -H "X-Auth-Token: $TOKEN" \
         -H "Content-Type: application/json" \
         -d '{"comments": [
             {
                 "original_text": "company is awesome!",
                 "timestamp": "2011-12-11T01:02:03.000000+00:00",
                 "id": "0123456789abcdef"
             },
             {
                 "original_text": "No, it is not...",
                 "timestamp": "2011-12-11T02:03:04.000000+00:00",
                 "id": "abcdef0123456789",
                 "user_properties": {
                     "string:email": "dorian@greyindustries.co.uk",
                     "number:age": 20
                 }
             }
         ]}'
```

This API is documented on the API docs page on `reinfer.io`.

This repo defines a very bare-bones client type for re:infer integrations in
`client.py` and uses it for a real-time (online) integration with a fake data
source in `online.py`.
