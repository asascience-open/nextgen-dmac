---
layout: default
parent: Data Storage and Discovery
nav_order: 3
---

# Catalog Queries

Catalog queries refers to the ability to search the metadata catalog. This design is somewhat dependent on the metadata format chosen, but doesn't have to be. For example, the STAC specification [defines specific methods for searching items](https://github.com/radiantearth/stac-api-spec/tree/v1.0.0-rc.1/item-search).

The metadata needs to be indexed by a search engine to be searchable. Only the metadata that is indexed can be searched.