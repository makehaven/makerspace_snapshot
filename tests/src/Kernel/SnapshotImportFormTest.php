<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\Core\File\FileSystemInterface;
use Drupal\Core\Form\FormState;
use Drupal\file\Entity\File;
use Drupal\KernelTests\KernelTestBase;
use Drupal\makerspace_snapshot\Form\SnapshotImportForm;

/**
 * Covers snapshot CSV import form behavior.
 *
 * @group makerspace_snapshot
 */
class SnapshotImportFormTest extends KernelTestBase {

  /**
   * {@inheritdoc}
   */
  protected static $modules = [
    'system',
    'user',
    'file',
    'makerspace_snapshot',
  ];

  /**
   * {@inheritdoc}
   */
  protected function setUp(): void {
    parent::setUp();

    $this->installEntitySchema('file');
    $this->installSchema('file', ['file_usage']);
    $this->installSchema('makerspace_snapshot', [
      'ms_snapshot',
      'ms_fact_membership_type_snapshot',
      'ms_fact_donation_snapshot',
    ]);

    // Seed one membership type column so CSV headers are deterministic.
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $property = new \ReflectionProperty($snapshotService, 'membershipTypeTerms');
    $property->setAccessible(TRUE);
    $property->setValue($snapshotService, [
      1 => ['label' => 'Individual'],
    ]);
  }

  /**
   * Ensures managed_file uses modern FileExtension validator.
   */
  public function testUploadValidatorUsesFileExtensionConstraint(): void {
    $formObject = SnapshotImportForm::create($this->container);
    $form = $formObject->buildForm([], new FormState());

    $element = $form['datasets']['membership_types']['membership_types_csv'] ?? [];
    $validators = $element['#upload_validators'] ?? [];

    $this->assertArrayHasKey('FileExtension', $validators);
    $this->assertSame('csv', $validators['FileExtension']['extensions'] ?? NULL);
    $this->assertArrayNotHasKey('file_validate_extensions', $validators);
  }

  /**
   * Imports membership type and donation metric CSVs via form handlers.
   */
  public function testValidateAndSubmitImportsDatasets(): void {
    $formObject = SnapshotImportForm::create($this->container);
    $formState = new FormState();
    $form = $formObject->buildForm([], $formState);

    $membershipCsv = implode("\n", [
      'snapshot_date,members_total,membership_type_1',
      '2026-02-10,99,99',
    ]) . "\n";
    $donationCsv = implode("\n", [
      'snapshot_date,period_year,period_month,donors_count,ytd_unique_donors,contributions_count,recurring_contributions_count,onetime_contributions_count,recurring_donors_count,onetime_donors_count,first_time_donors_count,total_amount,recurring_amount,onetime_amount',
      '2026-02-15,2026,1,10,11,12,2,10,2,8,1,1234.56,200.00,1034.56',
    ]) . "\n";

    $membershipFile = $this->createManagedCsv($membershipCsv, 'membership_types.csv');
    $donationFile = $this->createManagedCsv($donationCsv, 'donation_metrics.csv');

    $formState->setValue('import_schedule', 'monthly');
    $formState->setValue('membership_types_csv', [$membershipFile->id()]);
    $formState->setValue('donation_metrics_csv', [$donationFile->id()]);

    $formObject->validateForm($form, $formState);
    $this->assertSame([], $formState->getErrors());

    $formObject->submitForm($form, $formState);

    $membershipSnapshot = $this->container->get('database')->select('ms_snapshot', 's')
      ->fields('s', ['id', 'snapshot_date', 'source'])
      ->condition('definition', 'membership_types')
      ->execute()
      ->fetchAssoc();
    $this->assertNotEmpty($membershipSnapshot);
    $this->assertSame('2026-02-01', $membershipSnapshot['snapshot_date']);
    $this->assertSame('manual_import', $membershipSnapshot['source']);

    $donationSnapshot = $this->container->get('database')->select('ms_snapshot', 's')
      ->fields('s', ['id', 'snapshot_date', 'source'])
      ->condition('definition', 'donation_metrics')
      ->execute()
      ->fetchAssoc();
    $this->assertNotEmpty($donationSnapshot);
    $this->assertSame('2026-02-01', $donationSnapshot['snapshot_date']);
    $this->assertSame('manual_import', $donationSnapshot['source']);

    $typeFact = $this->container->get('database')->select('ms_fact_membership_type_snapshot', 'mt')
      ->fields('mt', ['member_count', 'members_total'])
      ->condition('snapshot_id', (int) $membershipSnapshot['id'])
      ->condition('term_id', 1)
      ->execute()
      ->fetchAssoc();
    $this->assertNotEmpty($typeFact);
    $this->assertSame(99, (int) $typeFact['member_count']);
    $this->assertSame(99, (int) $typeFact['members_total']);

    $donationFact = $this->container->get('database')->select('ms_fact_donation_snapshot', 'd')
      ->fields('d', ['period_year', 'period_month', 'donors_count', 'total_amount'])
      ->condition('snapshot_id', (int) $donationSnapshot['id'])
      ->execute()
      ->fetchAssoc();
    $this->assertNotEmpty($donationFact);
    $this->assertSame(2026, (int) $donationFact['period_year']);
    $this->assertSame(1, (int) $donationFact['period_month']);
    $this->assertSame(10, (int) $donationFact['donors_count']);
    $this->assertSame('1234.56', (string) $donationFact['total_amount']);
  }

  /**
   * Creates a managed file entity with CSV contents.
   */
  protected function createManagedCsv(string $contents, string $filename): File {
    $uri = 'temporary://makerspace_snapshot_tests/' . $filename;
    $fileSystem = $this->container->get('file_system');
    $directory = 'temporary://makerspace_snapshot_tests';
    $fileSystem->prepareDirectory($directory, FileSystemInterface::CREATE_DIRECTORY | FileSystemInterface::MODIFY_PERMISSIONS);
    $fileSystem->saveData($contents, $uri, FileSystemInterface::EXISTS_REPLACE);

    $file = File::create([
      'uri' => $uri,
      'status' => 0,
    ]);
    $file->save();

    return $file;
  }

}
