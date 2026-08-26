use anyhow::{Context, Result, bail, ensure};
use miniscript::bitcoin::NetworkKind;
use miniscript::bitcoin::secp256k1::Secp256k1;
use miniscript::descriptor::{Descriptor, DescriptorPublicKey, XKeyNetwork, checksum};
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct DescriptorSet {
    branches: Vec<Descriptor<DescriptorPublicKey>>,
    ranged: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedScript {
    pub branch: u32,
    pub index: u32,
    pub scriptpubkey: String,
}

impl DescriptorSet {
    pub fn parse_checked(descriptor: &str, network: &str) -> Result<Self> {
        ensure!(
            descriptor.rsplit_once('#').is_some(),
            "descriptor checksum is required"
        );
        let body = checksum::verify_checksum(descriptor)
            .context("descriptor checksum verification failed")?;
        ensure!(
            body.len() + 9 == descriptor.len(),
            "descriptor checksum is required"
        );

        let parsed = Descriptor::<DescriptorPublicKey>::from_str(descriptor)
            .context("invalid output descriptor")?;
        parsed
            .sanity_check()
            .context("descriptor failed miniscript sanity checks")?;
        validate_network(&parsed, network)?;

        let ranged = parsed.has_wildcard();
        let branches = parsed
            .into_single_descriptors()
            .context("invalid multipath descriptor")?;
        ensure!(
            !branches.is_empty(),
            "descriptor has no derivation branches"
        );

        Ok(Self { branches, ranged })
    }

    pub fn is_ranged(&self) -> bool {
        self.ranged
    }

    pub fn branch_count(&self) -> usize {
        self.branches.len()
    }

    pub fn derive_range(&self, start: u32, end: u32) -> Result<Vec<DerivedScript>> {
        ensure!(start <= end, "invalid derivation range {start}..{end}");
        if !self.ranged {
            ensure!(
                start == 0 && end <= 1,
                "non-ranged descriptors have one script"
            );
        }

        let secp = Secp256k1::verification_only();
        let mut scripts = Vec::with_capacity(
            self.branches.len() * usize::try_from(end.saturating_sub(start)).unwrap_or(0),
        );
        for (branch, descriptor) in self.branches.iter().enumerate() {
            for index in start..end {
                let definite = descriptor
                    .at_derivation_index(index)
                    .with_context(|| format!("unable to derive branch {branch} index {index}"))?;
                let concrete = definite.derived_descriptor(&secp);
                scripts.push(DerivedScript {
                    branch: u32::try_from(branch).context("too many descriptor branches")?,
                    index,
                    scriptpubkey: hex::encode(concrete.script_pubkey().as_bytes()),
                });
            }
        }
        Ok(scripts)
    }

    pub fn derive_one(&self, branch: u32, index: u32) -> Result<DerivedScript> {
        let descriptor = self
            .branches
            .get(usize::try_from(branch).context("invalid descriptor branch")?)
            .with_context(|| format!("descriptor branch {branch} does not exist"))?;
        if !self.ranged {
            ensure!(index == 0, "non-ranged descriptors only have index zero");
        }
        let secp = Secp256k1::verification_only();
        let concrete = descriptor
            .at_derivation_index(index)
            .with_context(|| format!("unable to derive branch {branch} index {index}"))?
            .derived_descriptor(&secp);
        Ok(DerivedScript {
            branch,
            index,
            scriptpubkey: hex::encode(concrete.script_pubkey().as_bytes()),
        })
    }
}

fn validate_network(descriptor: &Descriptor<DescriptorPublicKey>, network: &str) -> Result<()> {
    let expected = if network == "bitcoin" {
        NetworkKind::Main
    } else {
        NetworkKind::Test
    };
    match descriptor.xkey_network() {
        XKeyNetwork::NoXKeys => Ok(()),
        XKeyNetwork::Mixed => bail!("descriptor contains mixed mainnet and testnet extended keys"),
        XKeyNetwork::Single(actual) if actual == expected => Ok(()),
        XKeyNetwork::Single(NetworkKind::Main) => {
            bail!("mainnet extended key cannot be used on CLN network '{network}'")
        }
        XKeyNetwork::Single(NetworkKind::Test) => {
            bail!("testnet extended key cannot be used on CLN network '{network}'")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_checksum(body: &str) -> String {
        Descriptor::<DescriptorPublicKey>::from_str(body)
            .unwrap()
            .to_string()
    }

    #[test]
    fn checksum_is_mandatory_and_verified() {
        let body = "wpkh(0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798)";
        assert!(DescriptorSet::parse_checked(body, "regtest").is_err());
        let checked = with_checksum(body);
        assert!(DescriptorSet::parse_checked(&checked, "regtest").is_ok());
        let mut bad = checked;
        bad.pop();
        bad.push('q');
        assert!(DescriptorSet::parse_checked(&bad, "regtest").is_err());
    }

    #[test]
    fn static_descriptor_has_exactly_one_script() {
        let descriptor = with_checksum(
            "wpkh(0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798)",
        );
        let set = DescriptorSet::parse_checked(&descriptor, "regtest").unwrap();
        assert!(!set.is_ranged());
        assert_eq!(set.derive_range(0, 1).unwrap().len(), 1);
        assert!(set.derive_range(0, 2).is_err());
    }

    #[test]
    fn derives_ranged_descriptor() {
        let body = "tr(xpub6BgBgsespWvERF3LHQu6CnqdvfEvtMcQjYrcRzx53QJjSxarj2afYWcLteoGVky7D3UKDP9QyrLprQ3VCECoY49yfdDEHGCtMMj92pReUsQ/0/*)";
        let descriptor = with_checksum(body);
        let set = DescriptorSet::parse_checked(&descriptor, "bitcoin").unwrap();
        assert!(set.is_ranged());
        assert_eq!(set.derive_range(0, 3).unwrap().len(), 3);
    }
}
