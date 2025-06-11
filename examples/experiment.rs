use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Clone)]
pub struct Foo {}

impl Foo {
    pub async fn run(_foo: Data<Foo>) -> Result<(), SystemError> {
        println!("Running foo system with Foo data");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Bar {}

impl Bar {
    pub async fn run(_bar: Data<Bar>) -> Result<(), SystemError> {
        println!("Running bar system with Bar data");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Baz {}

impl Baz {
    pub async fn run(_baz: Data<Baz>) -> Result<(), SystemError> {
        println!("Running baz system with Baz data");
        tokio::time::sleep(std::time::Duration::from_millis(175)).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Qux {}

impl Qux {
    pub async fn run(_qux: Data<Qux>) -> Result<(), SystemError> {
        println!("Running qux system with Qux data");
        tokio::time::sleep(std::time::Duration::from_millis(125)).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Quux {}

impl Quux {
    pub async fn run(_quux: Data<Quux>) -> Result<(), SystemError> {
        println!("Running quux system with Quux data");
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        Ok(())
    }
}

#[derive(Debug)]
pub enum SystemError {
    Generic(String),
    Timeout,
}

#[derive(Clone)]
pub struct Data<T> {
    value: T,
}

impl<T> Data<T> {
    fn new(value: T) -> Self {
        Data { value }
    }
}

trait System<E> {
    fn run<'a>(
        &'a self,
        runner: &'a Runner<E>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + 'a>>;
}

trait SystemFunction<Args, E> {
    fn into_system(self) -> Box<dyn System<E>>;
}

struct SystemWrapper0<F, E> {
    func: F,
    _phantom: std::marker::PhantomData<E>,
}

impl<F, Fut, E> System<E> for SystemWrapper0<F, E>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    E: 'static,
{
    fn run<'a>(
        &'a self,
        _runner: &'a Runner<E>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + 'a>> {
        Box::pin(async move { (self.func)().await })
    }
}

struct SystemWrapper1<F, T1, E> {
    func: F,
    _phantom: std::marker::PhantomData<(T1, E)>,
}

impl<F, T1, Fut, E> System<E> for SystemWrapper1<F, T1, E>
where
    F: Fn(Data<T1>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    E: 'static,
{
    fn run<'a>(
        &'a self,
        runner: &'a Runner<E>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + 'a>> {
        Box::pin(async move {
            if let Some(t1) = runner.get::<T1>() {
                (self.func)(t1).await
            } else {
                Ok(())
            }
        })
    }
}

struct SystemWrapper2<F, T1, T2, E> {
    func: F,
    _phantom: std::marker::PhantomData<(T1, T2, E)>,
}

impl<F, T1, T2, Fut, E> System<E> for SystemWrapper2<F, T1, T2, E>
where
    F: Fn(Data<T1>, Data<T2>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    E: 'static,
{
    fn run<'a>(
        &'a self,
        runner: &'a Runner<E>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + 'a>> {
        Box::pin(async move {
            if let (Some(t1), Some(t2)) = (runner.get::<T1>(), runner.get::<T2>()) {
                (self.func)(t1, t2).await
            } else {
                Ok(())
            }
        })
    }
}

struct SystemWrapper3<F, T1, T2, T3, E> {
    func: F,
    _phantom: std::marker::PhantomData<(T1, T2, T3, E)>,
}

impl<F, T1, T2, T3, Fut, E> System<E> for SystemWrapper3<F, T1, T2, T3, E>
where
    F: Fn(Data<T1>, Data<T2>, Data<T3>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    T3: 'static + Clone,
    E: 'static,
{
    fn run<'a>(
        &'a self,
        runner: &'a Runner<E>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + 'a>> {
        Box::pin(async move {
            if let (Some(t1), Some(t2), Some(t3)) =
                (runner.get::<T1>(), runner.get::<T2>(), runner.get::<T3>())
            {
                (self.func)(t1, t2, t3).await
            } else {
                Ok(())
            }
        })
    }
}

struct SystemWrapper4<F, T1, T2, T3, T4, E> {
    func: F,
    _phantom: std::marker::PhantomData<(T1, T2, T3, T4, E)>,
}

impl<F, T1, T2, T3, T4, Fut, E> System<E> for SystemWrapper4<F, T1, T2, T3, T4, E>
where
    F: Fn(Data<T1>, Data<T2>, Data<T3>, Data<T4>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    T3: 'static + Clone,
    T4: 'static + Clone,
    E: 'static,
{
    fn run<'a>(
        &'a self,
        runner: &'a Runner<E>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + 'a>> {
        Box::pin(async move {
            if let (Some(t1), Some(t2), Some(t3), Some(t4)) = (
                runner.get::<T1>(),
                runner.get::<T2>(),
                runner.get::<T3>(),
                runner.get::<T4>(),
            ) {
                (self.func)(t1, t2, t3, t4).await
            } else {
                Ok(())
            }
        })
    }
}

struct SystemWrapper5<F, T1, T2, T3, T4, T5, E> {
    func: F,
    _phantom: std::marker::PhantomData<(T1, T2, T3, T4, T5, E)>,
}

impl<F, T1, T2, T3, T4, T5, Fut, E> System<E> for SystemWrapper5<F, T1, T2, T3, T4, T5, E>
where
    F: Fn(Data<T1>, Data<T2>, Data<T3>, Data<T4>, Data<T5>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    T3: 'static + Clone,
    T4: 'static + Clone,
    T5: 'static + Clone,
    E: 'static,
{
    fn run<'a>(
        &'a self,
        runner: &'a Runner<E>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + 'a>> {
        Box::pin(async move {
            if let (Some(t1), Some(t2), Some(t3), Some(t4), Some(t5)) = (
                runner.get::<T1>(),
                runner.get::<T2>(),
                runner.get::<T3>(),
                runner.get::<T4>(),
                runner.get::<T5>(),
            ) {
                (self.func)(t1, t2, t3, t4, t5).await
            } else {
                Ok(())
            }
        })
    }
}

impl<F, Fut, E> SystemFunction<(), E> for F
where
    F: Fn() -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    E: 'static,
{
    fn into_system(self) -> Box<dyn System<E>> {
        Box::new(SystemWrapper0 {
            func: self,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<F, T1, Fut, E> SystemFunction<(T1,), E> for F
where
    F: Fn(Data<T1>) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    E: 'static,
{
    fn into_system(self) -> Box<dyn System<E>> {
        Box::new(SystemWrapper1 {
            func: self,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<F, T1, T2, Fut, E> SystemFunction<(T1, T2), E> for F
where
    F: Fn(Data<T1>, Data<T2>) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    E: 'static,
{
    fn into_system(self) -> Box<dyn System<E>> {
        Box::new(SystemWrapper2 {
            func: self,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<F, T1, T2, T3, Fut, E> SystemFunction<(T1, T2, T3), E> for F
where
    F: Fn(Data<T1>, Data<T2>, Data<T3>) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    T3: 'static + Clone,
    E: 'static,
{
    fn into_system(self) -> Box<dyn System<E>> {
        Box::new(SystemWrapper3 {
            func: self,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<F, T1, T2, T3, T4, Fut, E> SystemFunction<(T1, T2, T3, T4), E> for F
where
    F: Fn(Data<T1>, Data<T2>, Data<T3>, Data<T4>) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    T3: 'static + Clone,
    T4: 'static + Clone,
    E: 'static,
{
    fn into_system(self) -> Box<dyn System<E>> {
        Box::new(SystemWrapper4 {
            func: self,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<F, T1, T2, T3, T4, T5, Fut, E> SystemFunction<(T1, T2, T3, T4, T5), E> for F
where
    F: Fn(Data<T1>, Data<T2>, Data<T3>, Data<T4>, Data<T5>) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), E>> + 'static,
    T1: 'static + Clone,
    T2: 'static + Clone,
    T3: 'static + Clone,
    T4: 'static + Clone,
    T5: 'static + Clone,
    E: 'static,
{
    fn into_system(self) -> Box<dyn System<E>> {
        Box::new(SystemWrapper5 {
            func: self,
            _phantom: std::marker::PhantomData,
        })
    }
}

struct Runner<E> {
    data: HashMap<TypeId, Box<dyn Any>>,
    systems: Vec<Box<dyn System<E>>>,
}

impl<E: 'static> Runner<E> {
    fn new() -> Self {
        Runner {
            data: HashMap::new(),
            systems: Vec::new(),
        }
    }

    fn add<T: 'static + Clone>(&mut self, data: Data<T>) {
        self.data.insert(TypeId::of::<T>(), Box::new(data.value));
    }

    fn get<T: 'static + Clone>(&self) -> Option<Data<T>> {
        self.data
            .get(&TypeId::of::<T>())
            .and_then(|data| data.downcast_ref::<T>())
            .map(|value| Data::new(value.clone()))
    }

    fn register<Args, F>(&mut self, system: F)
    where
        F: SystemFunction<Args, E>,
    {
        self.systems.push(system.into_system());
    }

    async fn run(&self) -> Result<(), E> {
        for system in &self.systems {
            system.run(self).await?;
        }
        Ok(())
    }
}

pub struct FooBarRunner {}

impl FooBarRunner {
    pub async fn run(_foo: Data<Foo>, _bar: Data<Bar>) -> Result<(), SystemError> {
        println!("Running foobar system with both Foo and Bar data");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Ok(())
    }
}

pub struct NoopRunner {}

impl NoopRunner {
    pub async fn run() -> Result<(), SystemError> {
        println!("Running noop system");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(())
    }
}

pub struct MultiParamRunner {}

impl MultiParamRunner {
    pub async fn run(
        _foo: Data<Foo>,
        _bar: Data<Bar>,
        _baz: Data<Baz>,
        _qux: Data<Qux>,
        _quux: Data<Quux>,
    ) -> Result<(), SystemError> {
        println!("Running system with 5 parameters!");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), SystemError> {
    let mut runner = Runner::<SystemError>::new();

    runner.add(Data::new(Foo {}));
    runner.add(Data::new(Bar {}));
    runner.add(Data::new(Baz {}));
    runner.add(Data::new(Qux {}));
    runner.add(Data::new(Quux {}));

    runner.register(Foo::run);
    runner.register(Bar::run);
    runner.register(Baz::run);
    runner.register(Qux::run);
    runner.register(Quux::run);
    runner.register(FooBarRunner::run);
    runner.register(NoopRunner::run);
    runner.register(MultiParamRunner::run);

    runner.run().await?;

    Ok(())
}
