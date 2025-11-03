<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Core\Routing\RouteBuilderInterface;
use Symfony\Component\HttpFoundation\Response;

/**
 * Controller for safe cache clearing.
 */
class SafeCacheClearController extends ControllerBase {

  /**
   * The router builder.
   *
   * @var \Drupal\Core\Routing\RouteBuilderInterface
   */
  protected $routerBuilder;

  /**
   * Constructs a new SafeCacheClearController object.
   *
   * @param \Drupal\Core\Routing\RouteBuilderInterface $router_builder
   *   The router builder.
   */
  public function __construct(RouteBuilderInterface $router_builder) {
    $this->routerBuilder = $router_builder;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('router.builder')
    );
  }

  /**
   * Clears the router cache.
   *
   * @return \Symfony\Component\HttpFoundation\Response
   *   A response object.
   */
  public function clearCache() {
    $this->routerBuilder->rebuild();
    return new Response($this->t('The routing cache has been cleared.'));
  }

}
