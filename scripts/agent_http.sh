# Record each logical request key before curl dispatch; curl retries retain it.
agent_curl() {
  local argument mutating=0 key url=''
  for argument in "$@"; do
    case "$argument" in
      -d|--data|--data-raw|--data-binary|--json|POST) mutating=1 ;;
      http://*|https://*) url="$argument" ;;
    esac
  done
  if [[ "$mutating" == 1 ]]; then
    key=$(python3 -c 'import uuid; print(uuid.uuid4())')
    printf '%s\t%s\n' "$key" "$url" >> "$OUT/request-keys.tsv"
    command curl -H "Idempotency-Key: $key" "$@"
  else
    command curl "$@"
  fi
}
