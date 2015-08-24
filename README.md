# Woothee filter plugin for Embulk[![Build Status](https://secure.travis-ci.org/toyama0919/embulk-filter-woothee.png?branch=master)](http://travis-ci.org/toyama0919/embulk-filter-woothee)

parse UserAgent strings and to filter/drop specified categories of user terminals.

see. [woothee/woothee-java](https://github.com/woothee/woothee-java)

## Overview

* **Plugin type**: filter

## Configuration

- **key_name**: target key name (string, required)
- **out_key_name**: out key name (string, default: agent_name)
- **out_key_category**: out key category (string, default: agent_category)
- **out_key_os**: out key os (string, default: agent_os)
- **out_key_version**: out key version (string, default: agent_version)
- **out_key_vendor**: out key vendor (string, default: agent_vendor)
- **filter_categories**: filter categories (array, default: null)
- **drop_categories**: drop categories (array, default: null)
- **merge_agent_info**: merge agent info (bool, default: false)

## Example1(add agent info)

```yaml
filters:
  - type: woothee
    key_name: user_agent
    merge_agent_info: true
out:
  type: stdout
```

## Example2(filter categories)

```yaml
filters:
  - type: woothee
    key_name: user_agent
    merge_agent_info: true
    filter_categories:
      - pc
      - smartphone
      - mobilephone
      - appliance
out:
  type: stdout
```

## Example3(drop categories)

```yaml
filters:
  - type: woothee
    key_name: user_agent
    merge_agent_info: true
    drop_categories:
      - crawler
      - UNKNOWN
out:
  type: stdout
```


## Build

```
$ ./gradlew gem
```
