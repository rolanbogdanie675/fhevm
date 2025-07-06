import { FHE } from "@fhevm/solidity";
import { SepoliaConfig } from "@fhevm/solidity/config/ZamaConfig.sol";

constructor () {
  FHE.setCoprocessor(0x848B0066793BcC60346Da1F49049357399B8D595);
  FHE.setDecryptionOracle(0xa02Cda4Ca3a71D7C46997716F4283aa851C28812);
}

function requestBoolInfinite() public {
  bytes32[] memory cts = new bytes32[](1);
  cts[0] = FHE.toBytes32(myEncryptedValue);
  FHE.requestDecryption(cts, this.myCallback.selector);
}
