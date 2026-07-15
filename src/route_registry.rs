use axum::{extract::State, handler::Handler};

use crate::{
    app::AppState,
    auth::{AdminGuard, CompanyWriterGuard, UserGuard},
};

/// Authentication/authorization guard required by a registered route.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RouteGuard {
    Public,
    User,
    CompanyWriter,
    Admin,
}

/// Static metadata generated alongside an Axum route registration.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RouteMetadata {
    pub method: &'static str,
    pub path: &'static str,
    pub handler: &'static str,
    pub guard: RouteGuard,
}

impl RouteMetadata {
    pub const fn new(
        method: &'static str,
        path: &'static str,
        handler: &'static str,
        guard: RouteGuard,
    ) -> Self {
        Self {
            method,
            path,
            handler,
            guard,
        }
    }
}

pub(crate) trait PublicRouteTuple {}
pub(crate) trait UserGuardRouteTuple {}
pub(crate) trait CompanyWriterGuardRouteTuple {}
pub(crate) trait AdminGuardRouteTuple {}

// Public handlers are intentionally allowlisted by extractor shape. Adding a new
// public shape therefore requires an explicit review instead of silently accepting
// an authentication guard as its first argument.
impl PublicRouteTuple for ((),) {}
impl<M> PublicRouteTuple for (M, State<AppState>) {}

// Axum represents a handler's arguments as `(M, Arg1, ..., Last)`. Cover every
// arity Axum 0.8 supports so the declared policy can require Arg1 to be its guard.
macro_rules! impl_guard_route_tuple {
    ($trait:ident, $guard:ty $(, $tail:ident)*) => {
        impl<M, $($tail,)*> $trait for (M, $guard, $($tail,)*) {}
    };
}

macro_rules! impl_guard_route_tuples {
    ($trait:ident, $guard:ty) => {
        impl_guard_route_tuple!($trait, $guard);
        impl_guard_route_tuple!($trait, $guard, T1);
        impl_guard_route_tuple!($trait, $guard, T1, T2);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5, T6);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5, T6, T7);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8, T9);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
        impl_guard_route_tuple!($trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
        impl_guard_route_tuple!(
            $trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13
        );
        impl_guard_route_tuple!(
            $trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14
        );
        impl_guard_route_tuple!(
            $trait, $guard, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15
        );
    };
}

impl_guard_route_tuples!(UserGuardRouteTuple, UserGuard);
impl_guard_route_tuples!(CompanyWriterGuardRouteTuple, CompanyWriterGuard);
impl_guard_route_tuples!(AdminGuardRouteTuple, AdminGuard);

// These identity functions add no middleware and never execute at request time;
// their bounds only make route-policy drift a compile error.
pub(crate) fn require_public_route<H, T>(handler: H) -> H
where
    H: Handler<T, AppState>,
    T: PublicRouteTuple,
{
    handler
}

pub(crate) fn require_user_guard<H, T>(handler: H) -> H
where
    H: Handler<T, AppState>,
    T: UserGuardRouteTuple,
{
    handler
}

pub(crate) fn require_company_writer_guard<H, T>(handler: H) -> H
where
    H: Handler<T, AppState>,
    T: CompanyWriterGuardRouteTuple,
{
    handler
}

pub(crate) fn require_admin_guard<H, T>(handler: H) -> H
where
    H: Handler<T, AppState>,
    T: AdminGuardRouteTuple,
{
    handler
}

macro_rules! require_declared_guard {
    (Public, $handler:ident) => {
        $crate::route_registry::require_public_route($handler)
    };
    (User, $handler:ident) => {
        $crate::route_registry::require_user_guard($handler)
    };
    (CompanyWriter, $handler:ident) => {
        $crate::route_registry::require_company_writer_guard($handler)
    };
    (Admin, $handler:ident) => {
        $crate::route_registry::require_admin_guard($handler)
    };
}

macro_rules! route_method {
    (get) => {
        "GET"
    };
    (post) => {
        "POST"
    };
    (put) => {
        "PUT"
    };
    (patch) => {
        "PATCH"
    };
    (delete) => {
        "DELETE"
    };
}

macro_rules! declare_routes {
    (
        $(
            $path:literal => $first_method:ident($first_handler:ident, $first_guard:ident)
                $(.$method:ident($handler:ident, $guard:ident))*;
        )+
    ) => {
        pub static REGISTERED_ROUTES: &[$crate::route_registry::RouteMetadata] = &[
            $(
                $crate::route_registry::RouteMetadata::new(
                    $crate::route_registry::route_method!($first_method),
                    $path,
                    stringify!($first_handler),
                    $crate::route_registry::RouteGuard::$first_guard,
                ),
                $(
                    $crate::route_registry::RouteMetadata::new(
                        $crate::route_registry::route_method!($method),
                        $path,
                        stringify!($handler),
                        $crate::route_registry::RouteGuard::$guard,
                    ),
                )*
            )+
        ];

        fn registered_router() -> Router<AppState> {
            Router::new()
                $(.route(
                    $path,
                    $first_method($crate::route_registry::require_declared_guard!(
                        $first_guard,
                        $first_handler
                    ))$(.$method($crate::route_registry::require_declared_guard!(
                        $guard,
                        $handler
                    )))*
                ))+
        }
    };
}

pub(crate) use declare_routes;
pub(crate) use require_declared_guard;
pub(crate) use route_method;
