param(
  [Parameter(Mandatory=$true)][string]$LabRoot,
  [Parameter(Mandatory=$true)][ValidateSet('cp','b','c')][string]$Role,
  [Parameter(Mandatory=$true)][string]$ExpectedRawBiosUuid,
  [string]$ConfirmToken,
  [switch]$Apply
)
$ErrorActionPreference='Stop'
$vmBase=[IO.Path]::GetFullPath((Join-Path $env:USERPROFILE 'Documents\Virtual Machines')).TrimEnd('\')
$root=(Resolve-Path -LiteralPath $LabRoot).ProviderPath.TrimEnd('\')
$rootItem=Get-Item -LiteralPath $root
if($rootItem.Parent.FullName -ne $vmBase -or !$rootItem.Name.StartsWith('Banking_DR_Isolated_')) {
  throw 'STOP: only the named isolated DR directory is allowed; original VM directories are protected.'
}
$target=(Resolve-Path -LiteralPath (Join-Path $root $Role)).ProviderPath.TrimEnd('\')
if((Split-Path -Parent $target) -ne $root) {throw 'STOP: target escaped isolated lab root.'}
$items=@(Get-Item -LiteralPath $root)+@(Get-Item -LiteralPath $target)+@(Get-ChildItem -LiteralPath $target -Force -Recurse)
if($items | Where-Object {($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0}) {throw 'STOP: reparse point in target tree.'}
$vmx=Join-Path $target ('banking-dr-'+$Role+'.vmx')
$contents=Get-Content -Raw -LiteralPath $vmx
$bios=[regex]::Match($contents,'(?m)^uuid\.bios\s*=\s*"([^"]+)"').Groups[1].Value
$normalize={param($value) [regex]::Replace($value,'[-\s]','').ToLowerInvariant()}
if(!$bios -or (& $normalize $bios) -ne (& $normalize $ExpectedRawBiosUuid)) {throw 'STOP: VM hardware identity changed.'}
$disks=@()
foreach($m in [regex]::Matches($contents,'(?m)^\S+\.fileName\s*=\s*"([^"]+\.vmdk)"')) {
  $disk=[IO.Path]::GetFullPath((Join-Path $target $m.Groups[1].Value))
  if(!$disk.StartsWith($target+'\',[StringComparison]::OrdinalIgnoreCase)) {throw 'STOP: external disk dependency.'}
  $disks+=$disk
}
if($disks.Count -lt 1) {throw 'STOP: no virtual disk was identified.'}
$token='DESTROY-ISOLATED-'+$Role.ToUpperInvariant()+':'+(& $normalize $bios)
$report=[ordered]@{status='DRY_RUN';target=$target;vmx=$vmx;raw_bios_uuid=$bios;disks=$disks;required_token=$token;original_vm_files_deleted=$false}
if(!$Apply) {$report | ConvertTo-Json -Depth 5; return}
if($ConfirmToken -cne $token) {throw 'STOP: exact target-specific confirmation token required.'}
$vmrun='C:\Program Files\VMware\VMware Workstation\vmrun.exe'
$running=& $vmrun list
if($LASTEXITCODE -ne 0) {throw 'Cannot inspect hypervisor state.'}
if($running -contains $vmx) {& $vmrun stop $vmx hard; if($LASTEXITCODE -ne 0) {throw 'VM power-off failed.'}}
$running=& $vmrun list
if($running -contains $vmx) {throw 'STOP: VM is still running.'}
# Re-resolve immediately before the only recursive deletion, in this same shell.
$verified=(Resolve-Path -LiteralPath $target).ProviderPath.TrimEnd('\')
if($verified -ne $target -or (Split-Path -Parent $verified) -ne $root) {throw 'STOP: target changed.'}
Remove-Item -LiteralPath $verified -Recurse -Force
if(Test-Path -LiteralPath $verified) {throw 'VM directory deletion did not complete.'}
$report.status='PERMANENT_VM_LOSS_INJECTED'
$report.injected_utc=[DateTime]::UtcNow.ToString('o')
$report | ConvertTo-Json -Depth 5
