# Voleur

## Intro

A tool for extracting, anonymizing and restoring data from/to PostgreSQL databases. It's
very useful for seeding local/development/staging environments with production-like data
without having to write or generate dummy data.

Voleur does not stream data from a source to a destination but instead opts for storing
the extracted data in S3 for later consumption. This middle layer gives us two benefits:

(1) Easy access control using existing IAM roles.
(2) The extracts become easily consumable by multiple parties without any additional
    tools or strain on the databases.

Currently, Voleur uses [Klepto](https://github.com/hellofresh/klepto) for data extraction
so it requires a valid Klepto configuration file to function.

## Installation

Voleur is distributed as a Python package but it's not on PyPI currently. You can install
with:

TODO

## Usage

Voleur has two concepts: **stashing** and **restoring**.

### Stashing

Stashing is the process of extracting, anonymizing and storing a structure/data dump from
a source PostgreSQL database to an S3 bucket. Each extracted dump is given a unique name
and can be optionally tagged for referring to it later on in a more humane manner. Tags
are *unique* across the S3 bucket.

To stash, run:

```
voleur stash <source> -b <bucket> [-t <tag>...] [-c <config>]
```

In the command above, `<source>` is the source database URI (`postgres://...`), `bucket`
is the S3 bucket and `-t <tag>...` the tags to apply.

Since Voleur uses Klepto under the hood, a Klepto config is required and will default to
`klepto.toml`. It can be overriden using the `-c` option.

#### Tagging best practices

If you are stashing dumps from multiple databases it's best if your prefix your tags with
the database name. For example: `master/2020-03-20` is a tag applied to a dump from the
`master` database on the 20th March 2020.

If you're running `voleur` nightly and you want an easy way to restore the latest dump
then then you can tag your dump as `latest` or `freshest` (or something that suits your
fancy). Since tags are unique, the existing `latest` tag will now point to the latest
dump.

### Restoring

TODO
