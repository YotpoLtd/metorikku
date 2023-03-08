{%- set array = [("default.accounts", "mocks/accounts.jsonl"), ("metadatas.accounts", "mocks/accounts_metadata.jsonl")] %}
{
  "metric": "metorikku-tester-test-multiple-databases.yaml",
  "mocks": [
    {%- for x in array %}
    {
      "name": "{{ x[0] }}",
      "path": "{{ x[1] }}"
    }{% if !loop.last %},{% endif %}
   {%- endfor  %}
  ],
  "tests": {
    "accountsDf": [
      {
        "app_key": "AAAA",
        "id": "A",
        "metadata": 1
      },
      {
        "app_key": "BBBB",
        "id": "B",
        "metadata": "2"
      },
      {
        "app_key": "CCCC",
        "id": "C",
        "metadata": "3"
      }
    ]
  }
}
