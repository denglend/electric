#!/bin/bash
shp2json shp_precinct_2012/Voting_Precinct__2012.shp -o dc.tmp.json
geoproject 'd3.geoConicConformal().parallels([38 + 18 / 60, 39 + 27 / 60]).rotate([77, -37 - 40 / 60]).fitSize([960,960],d)' < dc.tmp.json > dc_projected.tmp.json
