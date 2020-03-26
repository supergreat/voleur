# Voleur

## Intro

A tool for extracting, anonymizing and restoring data from/to PostgreSQL databases. It's
very useful for seeding local/development/staging environments with production-like data
without having to write or generate dummy data.

Voleur does not stream data from a source to a destination but instead opts for storing
the extracted data in S3 for later consumption. This middle layer gives us two benefits:

1. Easy access control using existing IAM roles.
2. The extracts become easily consumable by multiple parties without any additional
    tools or strain on the source databases.

Currently, Voleur uses [Klepto](https://github.com/hellofresh/klepto) for data extraction
so it requires a valid Klepto configuration file to function.

## Installation

Voleur is distributed as a Python package but it's not on PyPI currently. You can install
from GitHub with:

```
pip install -e git://github.com/supergreat/voleur.git@0.1#egg=voleur
```

## Usage

Voleur has two concepts: **stashing** and **restoring**.

### Stashing

Stashing is the process of extracting, anonymizing and storing a structure/data dump from
a source PostgreSQL database to an S3 bucket. Each extracted dump is given a unique ID
and can be optionally tagged for referring to it in a more humane manner. Tags
are *unique* across the S3 bucket.

To stash, run:

```
voleur stash <source> -b <bucket> [-t <tag>]... [-c <config>]
```

In the command above:

* `<source>` is the source database URI (`postgres://...`).
* `-b <bucket>` is the S3 bucket.
* `-t <tag>...` the tags to apply. You can add multiple tags: `-t foo -t bar`.

Since Voleur uses Klepto under the hood, a Klepto config is required and will default to
`klepto.toml`. It can be overriden using the `-c` option.

Voleur will print the dump unique ID along with the applied tags when it's done.

#### Tagging best practices

If you are stashing dumps from multiple databases (or multiple datasets from the same
database) it's best if your prefix your tags with the database or dataset name. For
example: `master/2020-03-20` is a tag applied to a dump from the `master` database on
the 20th March 2020.

If you're running `voleur` nightly and you want an easy way to restore the latest dump
then then you can add two tags to each dump:

* A date tag: `master/2020-03-20-2000` (gives you hourly )
* A static tag: `master/latest`. Since tags are unique, the existing `latest` tag will
now point to the latest dump.

### Restoring

Restoring takes a stashed dump and writes it to a target PostgreSQL database. The target
database should be empty, with no schema, as voleur will create the schema first.

To restore, run:

```
voleur restore <dump> <target> -b <bucket>
```

In the command above:

* `<dump>` is a dump unique ID or tag.
* `<target>` is the target database URI (`postgres://...`).
* `-b <bucket>` is the S3 bucket.
