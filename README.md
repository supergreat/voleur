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

Currently, Voleur uses [Klepto]() for data extraction so it requires a valid Klepto
configuration file to function.

## Installation

TODO

## Usage

TODO
