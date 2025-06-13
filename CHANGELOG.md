# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

<!-- insertion marker -->
## [0.3.0](https://github.com/SeqOIA-IT/sake_request/releases/tag/0.3.0) - 2025-06-13

<small>[Compare with 0.2.0](https://github.com/SeqOIA-IT/sake_request/compare/0.2.0...0.3.0)</small>

### Breaking

- drop support of python 3.9 ([0aec283](https://github.com/SeqOIA-IT/sake_request/commit/0aec283e1bc6b78c166461b46ed6bfd307ed52fa) by Pierre Marijon).

### Features

- better chromosome detection for add\_annotations and allow user to choose the chromosome name ([25c389](https://github.com/SeqOIA-IT/sake_request/commit/25c38981d091e7a936e10b3a642d66309b278653) by Pierre Marijon).
- interval method now allow user to add comment to each interval ([9ae9f8d](https://github.com/SeqOIA-IT/sake_request/commit/9ae9f8d21939372a3a6abf2190e12339db6d7189) by Pierre Marijon).

### Bug Fixes

- add\_recurence perform unique before compute ([c865c85](https://github.com/SeqOIA-IT/sake_request/commit/c865c85d450b2e28e1c4c4419fb061601b8b61e9) by Pierre Marijon).
- show utils submodule in documentation ([b661e37](https://github.com/SeqOIA-IT/sake_request/commit/b661e37f4e9b5a11eb6a302f5dd4c3d3f6cab73a) by Pierre Marijon).
- if preindication is empty produce preindication path correctly ([6f8ca8](https://github.com/SeqOIA-IT/sake_request/commit/6f8ca83ea117e62621fc701a6761f0b5c543b2ae) by Pierre Marijon).

### Code Refactoring

- assume unique variants are split by chromosomes ([42ba623](https://github.com/SeqOIA-IT/sake_request/commit/42ba6236a655988be27a05d2d04b281773aa9d54) by Pierre Marijon).

## [0.2.0](https://github.com/SeqOIA-IT/sake_request/releases/tag/0.2.0) - 2025-04-15

<small>[Compare with 0.1.0](https://github.com/SeqOIA-IT/sake_request/compare/0.1.0...0.2.0)</small>

### Features

- add method to get cnv ([68afdbc](https://github.com/SeqOIA-IT/sake_request/commit/68afdbcd03895aabf225d496ceb1660a76745ee2) by Pierre Marijon).
- create duckdb object in each threads ([c9132a1](https://github.com/SeqOIA-IT/sake_request/commit/c9132a1156f7a325662f34e7f0c8270066918b79) by Pierre Marijon).
- add ability of add genotype to scan file ([585dbd4](https://github.com/SeqOIA-IT/sake_request/commit/585dbd4cf7c2dc7380621b4597806e831da12781) by Pierre Marijon).
- support unique variants in one file or split by chromosome ([f9a801e](https://github.com/SeqOIA-IT/sake_request/commit/f9a801e6a1f0ba34c26fae5815859cae9af9ffac) by Pierre Marijon).

### Bug Fixes

- issue #3 ([4432ba8](https://github.com/SeqOIA-IT/sake_request/commit/4432ba83caff08d4b1eedeece2508653730f15af) by Pierre Marijon).
- genotype query not use left join ([9259900](https://github.com/SeqOIA-IT/sake_request/commit/925990075108e4cd009e34617cb2db23ee8787e7) by Pierre Marijon).
- simplify snpeff and variant2gene add_annotation ([53b0b3c](https://github.com/SeqOIA-IT/sake_request/commit/53b0b3c8ac1ea401ee53589c73973c56e76e08e3) by Pierre Marijon).
- #2 ([4c4fc82](https://github.com/SeqOIA-IT/sake_request/commit/4c4fc8206574a8b2e1ca767ce6b10f3482cff0d2) by Pierre Marijon).
- change in duckdb ([d94a2a9](https://github.com/SeqOIA-IT/sake_request/commit/d94a2a941de57e414b57a88dd1541506da9944e9) by Pierre Marijon).
- add test for all_variants ([28ddd29](https://github.com/SeqOIA-IT/sake_request/commit/28ddd295c2d035d59087759c81849c25109990d9) by Pierre Marijon).

### Code Refactoring

- transmissions use same code as genotyping and could be paralellize ([c14aa96](https://github.com/SeqOIA-IT/sake_request/commit/c14aa9678056f2eb4e9f7913819303ae99338334) by Pierre Marijon).
- move string query in specific module ([df7ac12](https://github.com/SeqOIA-IT/sake_request/commit/df7ac12ed3ff233d53e358e5e0b6419440620595) by Pierre Marijon).
- preindication target is set at object level not at method ([4c6fb43](https://github.com/SeqOIA-IT/sake_request/commit/4c6fb431703aa54a5bf0a874f0bf2622d01e82ca) by Pierre Marijon).
- move tqdm wrapper stuff in specific function ([0fc601f](https://github.com/SeqOIA-IT/sake_request/commit/0fc601f60588f2a730079ce0b5ed702b19d60920) by Pierre Marijon).

## [0.1.0](https://github.com/SeqOIA-IT/sake_request/releases/tag/0.1.0) - 2024-12-09

<small>[Compare with first commit](https://github.com/SeqOIA-IT/sake_request/compare/e371f8faefbd6a9ef16ece3cdfaabf9091bfdb2a...0.1.0)</small>

### Features

- add_genotypes support variables number of bits ([ef01687](https://github.com/SeqOIA-IT/sake_request/commit/ef01687b0f9cf534cda033ec5105f9ed29b00cbd) by Pierre Marijon).
- switch to github ([a6e96d5](https://github.com/SeqOIA-IT/sake_request/commit/a6e96d590df3976518091664269bab9b03fd1809) by Pierre Marijon).
- duckdb connection object is public ([cd5f822](https://github.com/SeqOIA-IT/sake_request/commit/cd5f8221027b56d0d55ec0ccd36f29ba9f0c6361) by Pierre Marijon).
- disable progress bar ([bd210c7](https://github.com/SeqOIA-IT/sake_request/commit/bd210c7c29cf567b428b2520d99599a4b5160357) by Pierre Marijon).
- add support of tqdm ([f3ede2d](https://github.com/SeqOIA-IT/sake_request/commit/f3ede2d1c4475fdf4d5ab71c82cd7b9a06198a3f) by Pierre Marijon).
- add code to support python3.9 ([f21c322](https://github.com/SeqOIA-IT/sake_request/commit/f21c3223c39c84ba6d8c0b70aecc2fe9e9da195d) by Pierre Marijon).
- add methode to get multiple interval ([b3c11e8](https://github.com/SeqOIA-IT/sake_request/commit/b3c11e883c2c8f3e994df203604593102a36f9a3) by Pierre Marijon).
- reach high test coverage ([178a73c](https://github.com/SeqOIA-IT/sake_request/commit/178a73c1ebcfe8e93f467193d84508b4055a9860) by Pierre Marijon).
- Sake threads value also control polars threads ([9e9ac60](https://github.com/SeqOIA-IT/sake_request/commit/9e9ac608a812f5ce162f8e39a8f44f8ed60674ec) by Pierre Marijon).
- Sake object could be create and unique variants are extractable ([0ee518e](https://github.com/SeqOIA-IT/sake_request/commit/0ee518e3cfeb242bfd6ca8106196a022beccebba) by Pierre Marijon).

### Bug Fixes

- if chromosome file isn't present durring annotations skip it ([f824c13](https://github.com/SeqOIA-IT/sake_request/commit/f824c13121dc79faeb1e4556e610edffb59350f7) by Pierre Marijon).
- better formating of code in usage ([47e3656](https://github.com/SeqOIA-IT/sake_request/commit/47e3656c65fdebad86c6c9ed393abf64342b4125) by Pierre Marijon).
- correct typo in utils function. ([faa8273](https://github.com/SeqOIA-IT/sake_request/commit/faa8273dd5da2ccd6f66242050578c29a7192ffe) by Pierre Marijon).
- test evaluate all path and number of thread ([dc462ba](https://github.com/SeqOIA-IT/sake_request/commit/dc462ba302366647c7c0723706b4b7a282f1990d) by Pierre Marijon).
