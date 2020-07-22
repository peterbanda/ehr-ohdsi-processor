# EHR OHDSI Processor [![version](https://img.shields.io/badge/version-0.4.1-green.svg)](https://peterbanda.net) [![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Build Status](https://travis-ci.com/peterbanda/ehr-ohdsi-processor.svg?branch=master)](https://travis-ci.com/peterbanda/ehr-ohdsi-processor)
 
- Single pass EHR (Electronic Health Record) feature generation and extraction pipeline using [OHDSI](https://ohdsi.org) standardized tables/csvs and concepts.
- The processor is implemented using [Akka](https://akka.io) streaming technology, which contributes to memory efficient, fast, and scalable asynchronous processing. The core abstraction is a so-called flow: an in-and-out foldable stream attached to a csv source with prefiltered values based on matching date time intervals. Flows for different features are bundled where the input parsed values are broadcasted, zipped and unzipped accordingly.
- Features are generated based on JSON-like settings in `application.conf`, which can be freely altered and rerun without touching or recompiling the code.
- Supported features types are:
  - _Counts_ - number of records per date interval.
  
  - _Distinct Counts_ - number of distinct values within a given column (e.g., concept ids and care sites) per date interval.
  
  - _Concept Category Counts_ - number of records whose concept ids for a given column belong to a given category per date interval.
  
  - _Concept Category Exist Flags_ - binary flag indicating whether there exists at least one record with a concept id for a given column belonging to a given category per date interval.
  
  - _Duration_ - duration from the first occurrence calculated per date interval.
  
  - _Sums_ - sum of values for a given column and date interval; used purely for drug quantity.
  
  - _Time-lag Features_ - records (for each table/csv file) are sorted by date, then time lags are calculated and compacted to mean, std, min, max, and dominant relative differential frequency (grouped to 5 bins: -2,-1,0,1,2) indicating the prevalent direction/acceleration of record dates.
  
  - _Comorbidity scores_ - linear comorbidity measure calculated based on Elixhuser's categories per date interval (reported to have good predictability for short-term mortality).  Two versions with different weights were used: AHRQ and van Walraven.
  
  - _Dynamically Calculated Comorbidity Scores_ - instead of using fixed weights as above, weights are dynamically calculated based on differential relative ratios of the dead patients (withing 6 months) vs others. Two versions were implemented (see bellow): the first one uses the same categories as Elixhauser, the second one takes into account only serious conditions.
  
  - _Non-Aggregate (Static) Features_ - these features are directly generated from `person.csv` and include `gender` (concepts binarized), `age_at_last_visit`, `year_of_birth`, and `month_of_birth`.
  
- Additionally, the processor supports custom date intervals counted from the last visit (for each person), such as last 6 months, last 5-3 years, etc. For each such date interval a set of features are generated.  

## Prerequisite

- JDK 1.8 or higher

## Build

To create an executable jar with all dependencies run

```
sbt assembly
```

This will produce a file such as `ehr-ohdsi-processor-assembly-0.4.1.jar`

## Usage

#### 1. Feature Generation

- basic feature generation

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -i=<input_folder> -o=<output_file_name>
```

- or without an output file (features.csv in the input folder will be used)

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -i=<input_folder>
```

- note the optional 'mode' option

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -mode=features -i=<input_folder> -o=<output_file_name>
```

- features generation using custom features, concept categories, or date intervals passed via '-Dconfig.file'

```
java  -Xms10g -Xmx10g -Xss1M -Dconfig.file=<my_custom_application.conf> -jar ehr-ohdsi-processor-assembly-0.4.1.jar -i=<input_folder> -o=<output_file_name>
```

- features generation with time-lag based features

```
java  -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -with_time_lags= -i=<input_folder> -o=<output_file_name>
```

- features generation with time-lag based features and dynamic scores' weights export

```
java  -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -with_time_lags= -i=<input_folder> -o=<output_file_name> -o-dyn_score_weights=<weight_file_name>
```

- features generation with time-lag based features and dynamic scores' weights import

```
java  -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -with_time_lags= -i=<input_folder> -o=<output_file_name> -i-dyn_score_weights=<weight_file_name>
```

#### 2. Standardization

- standardization with comma delimited input files (no spaces)

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -mode=std -i=<input_files> -o=<output_folder_name>
```

- or without an output folder (the respective input folders will be used)

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -mode=std -i=<input_files>
```

- standardization with additional stats output (the generated file is '<input-file>-std.stats')

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -mode=std -ostats= -i=<input_files> -o=<output_folder_name>
```

- standardization using explicitly passed stats as input (means + stds) 

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -mode=std -istats=<input_stats_file> -i=<input_files> -o=<output_folder_name>
```

- standardization including the time-lag based feautures 

```
java -Xms10g -Xmx10g -Xss1M -jar ehr-ohdsi-processor-assembly-0.4.1.jar -mode=std -with_time_lags= -i=<input_files> -o=<output_folder_name>
```

#### 3. Changing the logging configuration

- by passing a logback file 

```
-Dlogback.configurationFile=<path_to_logback.xml>
```